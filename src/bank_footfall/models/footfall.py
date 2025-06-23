"""Footfall model definitions."""

from datetime import datetime
from typing import Optional

from sqlmodel import Field, Relationship, SQLModel

from .base import Base, TimestampMixin


class FootfallBase(SQLModel):
    """Base footfall model with common fields."""
    
    branch_id: int = Field(foreign_key="branches.id", description="Branch ID")
    detector_id: str = Field(max_length=50, description="Detector identifier")
    timestamp: datetime = Field(description="Timestamp of the measurement")
    visitor_count: int = Field(ge=0, description="Number of visitors")
    entry_count: int = Field(ge=0, description="Number of entries")
    exit_count: int = Field(ge=0, description="Number of exits")


class Footfall(FootfallBase, TimestampMixin, table=True):
    """Footfall table model."""
    
    __tablename__ = "footfall"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Relationship with branch
    branch: Optional["Branch"] = Relationship(back_populates="footfall_records")


class FootfallCreate(FootfallBase):
    """Model for creating a new footfall record."""
    pass


class FootfallRead(FootfallBase):
    """Model for reading footfall data."""
    
    id: int
    created_at: str
    updated_at: Optional[str] = None


class FootfallUpdate(SQLModel):
    """Model for updating footfall data."""
    
    visitor_count: Optional[int] = Field(default=None, ge=0)
    entry_count: Optional[int] = Field(default=None, ge=0)
    exit_count: Optional[int] = Field(default=None, ge=0)