import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.schemas.schema import (
    AvailableTableEntry,
    ColumnResponse,
    IntrospectionResult,
    RelationshipResponse,
    TableDetailResponse,
    TableResponse,
)
from app.connectors.base_connector import ConnectorType
from app.db.session import get_db
from app.services import schema_service
from app.services.connection_service import get_connection
from app.services.setup_service import launch_background_embeddings

router = APIRouter(tags=["schemas"])


@router.post(
    "/connections/{connection_id}/introspect",
    response_model=IntrospectionResult,
)
async def introspect_connection(
    connection_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await schema_service.introspect_and_cache(db, connection_id)
    launch_background_embeddings(connection_id)
    return IntrospectionResult(**result)


@router.get(
    "/connections/{connection_id}/available-tables",
    response_model=list[AvailableTableEntry],
    summary="List available dbo tables for SQL Server connections",
    description=(
        "Returns all tables in the dbo schema (after auto-exclusion of TS_* and backup tables) "
        "directly from the live database — does NOT update the schema cache. "
        "Only available for SQL Server connections. Used by the Table Manager UI."
    ),
)
async def list_available_tables(
    connection_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    conn = await get_connection(db, connection_id)
    if conn.connector_type != ConnectorType.SQLSERVER:
        raise HTTPException(
            status_code=400,
            detail="available-tables is only supported for SQL Server connections.",
        )
    tables = await schema_service.get_available_tables_for_sqlserver(db, connection_id)
    return [AvailableTableEntry(**t) for t in tables]


@router.get(
    "/connections/{connection_id}/tables",
    response_model=list[TableResponse],
)
async def list_tables(
    connection_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    tables = await schema_service.get_tables(db, connection_id)
    return [
        TableResponse(
            id=t.id,
            schema_name=t.schema_name,
            table_name=t.table_name,
            table_type=t.table_type,
            comment=t.comment,
            row_count_estimate=t.row_count_estimate,
            column_count=len(t.columns),
            created_at=t.created_at,
        )
        for t in tables
    ]


@router.get(
    "/tables/{table_id}",
    response_model=TableDetailResponse,
)
async def get_table_detail(
    table_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    table = await schema_service.get_table_detail(db, table_id)

    columns = [
        ColumnResponse(
            id=c.id,
            column_name=c.column_name,
            data_type=c.data_type,
            is_nullable=c.is_nullable,
            is_primary_key=c.is_primary_key,
            default_value=c.default_value,
            comment=c.comment,
            ordinal_position=c.ordinal_position,
        )
        for c in sorted(table.columns, key=lambda c: c.ordinal_position)
    ]

    outgoing = [
        RelationshipResponse(
            constraint_name=r.constraint_name,
            source_table=table.table_name,
            source_column=r.source_column,
            target_table=r.target_table.table_name if r.target_table else "?",
            target_column=r.target_column,
        )
        for r in table.outgoing_relationships
    ]

    incoming = [
        RelationshipResponse(
            constraint_name=r.constraint_name,
            source_table=r.source_table.table_name if r.source_table else "?",
            source_column=r.source_column,
            target_table=table.table_name,
            target_column=r.target_column,
        )
        for r in table.incoming_relationships
    ]

    return TableDetailResponse(
        id=table.id,
        schema_name=table.schema_name,
        table_name=table.table_name,
        table_type=table.table_type,
        comment=table.comment,
        row_count_estimate=table.row_count_estimate,
        columns=columns,
        outgoing_relationships=outgoing,
        incoming_relationships=incoming,
    )
