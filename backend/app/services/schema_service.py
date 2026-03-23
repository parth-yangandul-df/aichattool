import uuid
from datetime import UTC, datetime

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.connectors.base_connector import ConnectorType, TableInfo
from app.connectors.connector_registry import get_or_create_connector
from app.core.exceptions import NotFoundError
from app.db.models.schema_cache import CachedColumn, CachedRelationship, CachedTable
from app.services.connection_service import get_connection, get_decrypted_connection_string

# -------------------------------------------------------------------------
# SQL Server auto-exclusion rules (applied before whitelist check)
# -------------------------------------------------------------------------
_SQLSERVER_DBO_SCHEMA = "dbo"

_SQLSERVER_EXCLUDE_PREFIXES = ("ts_",)          # case-insensitive
_SQLSERVER_EXCLUDE_SUBSTRINGS = ("backup", "bakup")  # case-insensitive


def _is_sqlserver_auto_excluded(table_name: str) -> bool:
    """Return True if the table should be silently dropped for SQL Server connections."""
    lower = table_name.lower()
    for prefix in _SQLSERVER_EXCLUDE_PREFIXES:
        if lower.startswith(prefix):
            return True
    for sub in _SQLSERVER_EXCLUDE_SUBSTRINGS:
        if sub in lower:
            return True
    return False


def apply_sqlserver_filters(
    tables: list[TableInfo],
    allowed_table_names: list[str] | None,
) -> list[TableInfo]:
    """Apply SQL Server-specific filtering rules to a list of TableInfo objects.

    Rules (in order):
      1. Keep only tables in the ``dbo`` schema.
      2. Auto-exclude tables matching the built-in patterns (TS_*, *backup*, *bakup*).
      3. If ``allowed_table_names`` is non-empty, restrict to that exact whitelist.

    This function is pure (no DB/IO) so it can be reused by both introspect_and_cache
    and the available-tables endpoint.
    """
    # Step 1: dbo only
    result = [t for t in tables if t.schema_name.lower() == _SQLSERVER_DBO_SCHEMA]

    # Step 2: auto-exclusion
    result = [t for t in result if not _is_sqlserver_auto_excluded(t.table_name)]

    # Step 3: whitelist (only applies when non-empty)
    if allowed_table_names:
        whitelist = {name.lower() for name in allowed_table_names}
        result = [
            t for t in result
            if f"{t.schema_name}.{t.table_name}".lower() in whitelist
        ]

    return result


async def introspect_and_cache(
    db: AsyncSession,
    connection_id: uuid.UUID,
) -> dict[str, int]:
    """Introspect a target database and cache the schema metadata.

    For SQL Server connections this applies additional filtering:
      - Only the ``dbo`` schema is introspected.
      - Tables matching auto-exclusion patterns (TS_*, *backup*, *bakup*) are skipped.
      - If the connection has a non-empty ``allowed_table_names`` whitelist, only those
        tables are cached.
    """
    conn = await get_connection(db, connection_id)
    connection_string = get_decrypted_connection_string(conn)
    is_sqlserver = conn.connector_type == ConnectorType.SQLSERVER

    connector = await get_or_create_connector(
        str(connection_id), conn.connector_type, connection_string
    )

    # Clear existing cached data for this connection
    await db.execute(
        delete(CachedRelationship).where(CachedRelationship.connection_id == connection_id)
    )
    await db.execute(
        delete(CachedTable).where(CachedTable.connection_id == connection_id)
    )
    await db.flush()

    # For SQL Server: only introspect dbo — no need to call introspect_schemas() at all.
    # For every other connector: use the original full-schema discovery.
    if is_sqlserver:
        schemas = [_SQLSERVER_DBO_SCHEMA]
    else:
        schemas = await connector.introspect_schemas()

    total_tables = 0
    total_columns = 0
    total_relationships = 0

    # Map of (schema, table_name) -> CachedTable for FK resolution
    table_map: dict[tuple[str, str], CachedTable] = {}

    for schema_name in schemas:
        raw_tables = await connector.introspect_tables(schema_name)

        # Apply SQL Server filters (dbo-only, auto-exclusion, whitelist)
        if is_sqlserver:
            tables_to_cache = apply_sqlserver_filters(
                raw_tables, conn.allowed_table_names
            )
        else:
            tables_to_cache = raw_tables

        for table_info in tables_to_cache:
            cached_table = CachedTable(
                connection_id=connection_id,
                schema_name=table_info.schema_name,
                table_name=table_info.table_name,
                table_type=table_info.table_type,
                comment=table_info.comment,
                row_count_estimate=table_info.row_count_estimate,
            )
            db.add(cached_table)
            await db.flush()  # Get the ID

            table_map[(schema_name, table_info.table_name)] = cached_table
            total_tables += 1

            for col_info in table_info.columns:
                cached_col = CachedColumn(
                    table_id=cached_table.id,
                    column_name=col_info.name,
                    data_type=col_info.data_type,
                    is_nullable=col_info.is_nullable,
                    is_primary_key=col_info.is_primary_key,
                    default_value=col_info.default_value,
                    comment=col_info.comment,
                    ordinal_position=col_info.ordinal_position,
                )
                db.add(cached_col)
                total_columns += 1

    await db.flush()

    # Now process foreign keys (only for tables that made it through the filter)
    for schema_name in schemas:
        raw_tables = await connector.introspect_tables(schema_name)

        if is_sqlserver:
            tables_to_process = apply_sqlserver_filters(
                raw_tables, conn.allowed_table_names
            )
        else:
            tables_to_process = raw_tables

        for table_info in tables_to_process:
            source_table = table_map.get((schema_name, table_info.table_name))
            if not source_table:
                continue

            for fk in table_info.foreign_keys:
                target_table = table_map.get((fk.referred_schema, fk.referred_table))
                if not target_table:
                    continue

                rel = CachedRelationship(
                    connection_id=connection_id,
                    constraint_name=fk.constraint_name,
                    source_table_id=source_table.id,
                    source_column=fk.column_name,
                    target_table_id=target_table.id,
                    target_column=fk.referred_column,
                )
                db.add(rel)
                total_relationships += 1

    # Update last_introspected_at
    conn.last_introspected_at = datetime.now(UTC)
    await db.flush()

    return {
        "tables_found": total_tables,
        "columns_found": total_columns,
        "relationships_found": total_relationships,
    }


async def get_available_tables_for_sqlserver(
    db: AsyncSession,
    connection_id: uuid.UUID,
) -> list[dict[str, str]]:
    """Return all dbo tables (after auto-exclusion) from the live DB — does NOT update cache.

    Used by the frontend Table Manager modal to show what tables are available to whitelist.
    Returns a list of dicts: [{"schema_name": "dbo", "table_name": "Customers"}, ...]
    """
    conn = await get_connection(db, connection_id)
    connection_string = get_decrypted_connection_string(conn)

    connector = await get_or_create_connector(
        str(connection_id), conn.connector_type, connection_string
    )

    raw_tables = await connector.introspect_tables(_SQLSERVER_DBO_SCHEMA)

    # Apply only step 1 (dbo) and step 2 (auto-exclusion) — NOT the whitelist.
    # The frontend needs the full candidate set so users can manage their whitelist.
    filtered = [t for t in raw_tables if not _is_sqlserver_auto_excluded(t.table_name)]

    return [
        {"schema_name": t.schema_name, "table_name": t.table_name}
        for t in filtered
    ]


async def get_tables(
    db: AsyncSession, connection_id: uuid.UUID
) -> list[CachedTable]:
    result = await db.execute(
        select(CachedTable)
        .where(CachedTable.connection_id == connection_id)
        .options(selectinload(CachedTable.columns))
        .order_by(CachedTable.schema_name, CachedTable.table_name)
    )
    return list(result.scalars().all())


async def get_table_detail(
    db: AsyncSession, table_id: uuid.UUID
) -> CachedTable:
    result = await db.execute(
        select(CachedTable)
        .where(CachedTable.id == table_id)
        .options(
            selectinload(CachedTable.columns),
            selectinload(CachedTable.outgoing_relationships).selectinload(
                CachedRelationship.target_table
            ),
            selectinload(CachedTable.incoming_relationships).selectinload(
                CachedRelationship.source_table
            ),
        )
    )
    table = result.scalar_one_or_none()
    if not table:
        raise NotFoundError("Table", str(table_id))
    return table
