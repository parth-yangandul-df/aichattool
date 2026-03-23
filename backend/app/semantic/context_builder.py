"""The Context Builder — orchestrates hybrid context selection.

This is the core intelligence of the product. It selects the minimal
relevant context for the LLM prompt, combining:
1. Embedding similarity search
2. Keyword matching
3. FK relationship expansion
4. Glossary/metric/dictionary resolution
"""

import logging
import uuid
from dataclasses import dataclass

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

from app.db.models.schema_cache import CachedColumn, CachedRelationship, CachedTable
from app.semantic.glossary_resolver import (
    ResolvedDictionary,
    ResolvedGlossary,
    ResolvedKnowledge,
    ResolvedMetric,
    ResolvedSampleQuery,
    find_similar_queries,
    resolve_dictionary,
    resolve_glossary,
    resolve_knowledge,
    resolve_metrics,
)
from app.semantic.prompt_assembler import assemble_prompt
from app.semantic.schema_linker import LinkedTable, find_relevant_tables
from app.services.embedding_service import embed_text


@dataclass
class BuiltContext:
    """The assembled context ready for the LLM."""
    prompt_context: str  # Formatted text to include in the LLM prompt
    tables: list[LinkedTable]
    glossary: list[ResolvedGlossary]
    metrics: list[ResolvedMetric]
    knowledge: list[ResolvedKnowledge]
    dictionaries: list[ResolvedDictionary]
    sample_queries: list[ResolvedSampleQuery]
    question_embedding: list[float] | None


async def build_context(
    db: AsyncSession,
    connection_id: uuid.UUID,
    question: str,
    dialect: str = "postgresql",
) -> BuiltContext:
    """Build the full context for an NL question.

    Steps:
    1. Embed the question
    2. Find relevant tables (hybrid: embedding + keyword + FK expansion)
    3. Resolve glossary terms
    4. Resolve metrics
    5. Get dictionary entries for columns in selected tables
    6. Find similar sample queries
    7. Get relationships between selected tables
    8. Assemble everything into a structured prompt
    """
    # Step 1: Embed the question (gracefully degrade to keyword-only if unavailable)
    question_embedding: list[float] | None = None
    try:
        question_embedding = await embed_text(question)
    except Exception:
        logger.warning(
            "Embedding generation failed — falling back to keyword-only context. "
            "Ensure the embedding model is pulled (e.g. ollama pull nomic-embed-text).",
            exc_info=True,
        )

    # Step 2: Find relevant tables
    tables = await find_relevant_tables(
        db, connection_id, question_embedding, question
    )

    # Step 3: Resolve glossary terms
    glossary = await resolve_glossary(
        db, connection_id, question, question_embedding
    )

    # Boost tables referenced by glossary terms
    table_names_in_context = {lt.table.table_name for lt in tables}
    for g in glossary:
        for ref_table in g.related_tables:
            if ref_table not in table_names_in_context:
                # Could fetch and add the table, but for now just note it
                pass

    # Step 4: Resolve metrics
    metrics = await resolve_metrics(
        db, connection_id, question, question_embedding
    )

    # Step 5: Resolve knowledge chunks
    knowledge = await resolve_knowledge(
        db, connection_id, question, question_embedding
    )

    # Step 6: Find similar sample queries
    sample_queries = await find_similar_queries(
        db, connection_id, question_embedding
    )

    # Step 7: FK-neighbour expansion
    # Pull in any lookup/dimension tables directly referenced by FK from the
    # selected tables but not yet in context.  This fixes the root cause of
    # the LLM guessing abbreviated column names (e.g. [Name] instead of
    # [BusinessUnitName]) — the LLM can only use exact names if the neighbour
    # table's column list is present in the prompt.
    tables = await _expand_fk_neighbours(db, connection_id, tables, max_extra=5)

    # Step 8: Get dictionary entries for all columns (including FK-expanded tables)
    # and relationships between the final table set
    table_ids = [lt.table.id for lt in tables]
    column_ids = [col.id for lt in tables for col in lt.columns]
    dictionaries = await resolve_dictionary(db, column_ids)
    relationships = await _get_relationships_between(db, table_ids)

    # Step 9: Assemble prompt
    prompt_context = assemble_prompt(
        tables=tables,
        glossary=glossary,
        metrics=metrics,
        knowledge=knowledge,
        dictionaries=dictionaries,
        sample_queries=sample_queries,
        relationships=relationships,
        dialect=dialect,
    )

    return BuiltContext(
        prompt_context=prompt_context,
        tables=tables,
        glossary=glossary,
        metrics=metrics,
        knowledge=knowledge,
        dictionaries=dictionaries,
        sample_queries=sample_queries,
        question_embedding=question_embedding,
    )


async def _get_relationships_between(
    db: AsyncSession,
    table_ids: list[uuid.UUID],
) -> list[dict]:
    """Get all FK relationships between the given tables."""
    if len(table_ids) < 2:
        return []

    result = await db.execute(
        select(CachedRelationship).where(
            CachedRelationship.source_table_id.in_(table_ids),
            CachedRelationship.target_table_id.in_(table_ids),
        )
    )

    relationships = []
    for rel in result.scalars().all():
        # Need table names — load them
        source = await db.get(type(rel).source_table.property.entity.class_, rel.source_table_id)
        target = await db.get(type(rel).target_table.property.entity.class_, rel.target_table_id)
        if source and target:
            relationships.append({
                "source_table": source.table_name,
                "source_column": rel.source_column,
                "target_table": target.table_name,
                "target_column": rel.target_column,
            })

    return relationships


async def _expand_fk_neighbours(
    db: AsyncSession,
    connection_id: uuid.UUID,
    tables: list[LinkedTable],
    max_extra: int = 5,
) -> list[LinkedTable]:
    """Expand context by pulling in FK-neighbour tables not yet selected.

    For every table already in `tables`, find ALL FK relationships where it is
    either the source or the target.  Load any referenced table that is not
    already in context and append it as a LinkedTable with match_reason
    "fk_neighbour".  This ensures lookup/dimension tables (e.g. BusinessUnit,
    Designation, TechCategory) are always present so the LLM can read their
    exact column names.

    Args:
        db: Async SQLAlchemy session.
        connection_id: The connection whose schema is being queried.
        tables: The tables already selected by find_relevant_tables.
        max_extra: Cap on how many extra tables to add (prevents context explosion).

    Returns:
        Augmented list of LinkedTable (original tables + FK neighbours).
    """
    if not tables:
        return tables

    selected_ids = {lt.table.id for lt in tables}

    # Find ALL FK edges touching any selected table (source OR target side)
    rel_result = await db.execute(
        select(CachedRelationship).where(
            CachedRelationship.connection_id == connection_id,
            or_(
                CachedRelationship.source_table_id.in_(selected_ids),
                CachedRelationship.target_table_id.in_(selected_ids),
            ),
        )
    )
    relationships = rel_result.scalars().all()

    # Collect neighbour table IDs not already selected
    neighbour_ids: list[uuid.UUID] = []
    seen: set[uuid.UUID] = set()
    for rel in relationships:
        for candidate_id in (rel.source_table_id, rel.target_table_id):
            if candidate_id not in selected_ids and candidate_id not in seen:
                seen.add(candidate_id)
                neighbour_ids.append(candidate_id)

    if not neighbour_ids:
        return tables

    # Cap to avoid context explosion
    neighbour_ids = neighbour_ids[:max_extra]

    extra: list[LinkedTable] = []
    for table_id in neighbour_ids:
        cached_table = await db.get(CachedTable, table_id)
        if not cached_table:
            continue

        col_result = await db.execute(
            select(CachedColumn)
            .where(CachedColumn.table_id == table_id)
            .order_by(CachedColumn.ordinal_position)
        )
        columns = list(col_result.scalars().all())

        extra.append(LinkedTable(
            table=cached_table,
            columns=columns,
            score=0.1,
            match_reason="fk_neighbour",
        ))
        logger.debug(
            "FK-neighbour expansion: added table %s", cached_table.table_name
        )

    if extra:
        logger.info(
            "FK-neighbour expansion added %d table(s): %s",
            len(extra),
            [lt.table.table_name for lt in extra],
        )

    return tables + extra
