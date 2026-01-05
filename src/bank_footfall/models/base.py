"""Base model configuration."""

from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel


class TimestampMixin(SQLModel):
    """Mixin for adding timestamp fields to models."""

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default=None)


class Base(SQLModel):
    """Base model class."""

    pass
