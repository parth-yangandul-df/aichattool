import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class DatabaseConnection(Base):
    __tablename__ = "database_connections"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    connector_type: Mapped[str] = mapped_column(String(50), nullable=False)
    # Connection string stored encrypted; handled at service layer
    connection_string_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    default_schema: Mapped[str] = mapped_column(String(255), default="public")
    read_only: Mapped[bool] = mapped_column(Boolean, default=True)
    max_query_timeout_seconds: Mapped[int] = mapped_column(Integer, default=30)
    max_rows: Mapped[int] = mapped_column(Integer, default=1000)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_introspected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # SQL Server only: explicit whitelist of "schema.table" names to include during introspection.
    # Null or empty list = no whitelist (all dbo tables, minus auto-excluded ones, are included).
    allowed_table_names: Mapped[list[str] | None] = mapped_column(
        JSONB, nullable=True, default=None
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    tables: Mapped[list["CachedTable"]] = relationship(  # noqa: F821
        back_populates="connection", cascade="all, delete-orphan"
    )
    glossary_terms: Mapped[list["GlossaryTerm"]] = relationship(  # noqa: F821
        back_populates="connection", cascade="all, delete-orphan"
    )
    metric_definitions: Mapped[list["MetricDefinition"]] = relationship(  # noqa: F821
        back_populates="connection", cascade="all, delete-orphan"
    )
    sample_queries: Mapped[list["SampleQuery"]] = relationship(  # noqa: F821
        back_populates="connection", cascade="all, delete-orphan"
    )
