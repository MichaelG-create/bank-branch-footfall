# src/bank_footfall/models/footfall.py
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlmodel import Field, Relationship, SQLModel

from .base import Base, TimestampMixin

if TYPE_CHECKING:
    from .branch import Branch


class FootfallBase(SQLModel):
    branch_id: int = Field(foreign_key="branches.id", description="Branch ID")
    detector_id: str = Field(max_length=50, description="Detector identifier")
    timestamp: datetime = Field(description="Timestamp of the measurement")
    visitor_count: int = Field(ge=0, description="Number of visitors")
    entry_count: int = Field(ge=0, description="Number of entries")
    exit_count: int = Field(ge=0, description="Number of exits")


class Footfall(FootfallBase, TimestampMixin, Base, table=True):
    __tablename__ = "footfall"

    branch: Optional["Branch"] = Relationship(back_populates="footfall_records")


class FootfallCreate(FootfallBase):
    pass


class FootfallRead(FootfallBase):
    id: int
    created_at: str
    updated_at: Optional[str] = None


class FootfallUpdate(SQLModel):
    visitor_count: Optional[int] = Field(default=None, ge=0)
    entry_count: Optional[int] = Field(default=None, ge=0)
    exit_count: Optional[int] = Field(default=None, ge=0)
