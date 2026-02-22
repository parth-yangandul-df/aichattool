from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    connection_id: UUID
    question: str = Field(min_length=1, max_length=1000)


class ExecuteSQLRequest(BaseModel):
    connection_id: UUID
    sql: str = Field(min_length=1, max_length=10000)
    original_question: str | None = None  # for history tracking


class QueryResponse(BaseModel):
    id: UUID
    question: str
    generated_sql: str
    explanation: str
    columns: list[str]
    column_types: list[str]
    rows: list[list[Any]]
    row_count: int
    execution_time_ms: float
    truncated: bool
    summary: str | None
    suggested_followups: list[str]
    llm_provider: str
    llm_model: str


class SQLOnlyResponse(BaseModel):
    generated_sql: str
    explanation: str
    confidence: float
    tables_used: list[str]
    assumptions: list[str]


class QueryHistoryResponse(BaseModel):
    id: UUID
    connection_id: UUID
    natural_language: str
    generated_sql: str | None
    final_sql: str | None
    execution_status: str
    error_message: str | None
    row_count: int | None
    execution_time_ms: float | None
    retry_count: int
    result_summary: str | None
    is_favorite: bool
    created_at: datetime

    model_config = {"from_attributes": True}
