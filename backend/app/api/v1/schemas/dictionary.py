from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class DictionaryEntryCreate(BaseModel):
    raw_value: str = Field(min_length=1, max_length=255)
    display_value: str = Field(min_length=1, max_length=255)
    description: str | None = None
    sort_order: int = 0


class DictionaryEntryUpdate(BaseModel):
    raw_value: str | None = Field(default=None, min_length=1, max_length=255)
    display_value: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = None
    sort_order: int | None = None


class DictionaryEntryResponse(BaseModel):
    id: UUID
    column_id: UUID
    raw_value: str
    display_value: str
    description: str | None
    sort_order: int
    created_at: datetime

    model_config = {"from_attributes": True}
