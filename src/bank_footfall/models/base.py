# src/bank_footfall/models/base.py
from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Field, SQLModel


class TimestampMixin(SQLModel):
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = Field(default=None)


class Base(SQLModel):
    id: Optional[int] = Field(default=None, primary_key=True)
