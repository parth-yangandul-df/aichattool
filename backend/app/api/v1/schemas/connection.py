from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class ConnectionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    connector_type: str = Field(default="postgresql")
    connection_string: str = Field(min_length=1)
    default_schema: str = Field(default="public", max_length=255)
    max_query_timeout_seconds: int = Field(default=30, ge=1, le=300)
    max_rows: int = Field(default=1000, ge=1, le=100000)
    # SQL Server only: whitelist of exact "schema.table" names. Null/empty = include all.
    allowed_table_names: list[str] | None = Field(default=None)


class ConnectionUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    connection_string: str | None = Field(default=None, min_length=1)
    default_schema: str | None = Field(default=None, max_length=255)
    max_query_timeout_seconds: int | None = Field(default=None, ge=1, le=300)
    max_rows: int | None = Field(default=None, ge=1, le=100000)
    is_active: bool | None = None
    # SQL Server only: update the table whitelist. Pass empty list to clear it.
    allowed_table_names: list[str] | None = Field(default=None)


class ConnectionResponse(BaseModel):
    id: UUID
    name: str
    connector_type: str
    default_schema: str
    max_query_timeout_seconds: int
    max_rows: int
    is_active: bool
    has_connection_string: bool
    last_introspected_at: datetime | None
    created_at: datetime
    updated_at: datetime
    allowed_table_names: list[str] | None

    model_config = {"from_attributes": True}


class ConnectionTestResult(BaseModel):
    success: bool
    message: str
