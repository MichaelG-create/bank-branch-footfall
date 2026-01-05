"""Branch model definitions."""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from sqlmodel import Field, Relationship, SQLModel

from .base import TimestampMixin

if TYPE_CHECKING:
    from .footfall import Footfall  # only for type checking


class BranchBase(SQLModel):
    """Base branch model with common fields."""

    name: str = Field(max_length=100, description="Branch name")
    address: str = Field(max_length=200, description="Branch address")
    city: str = Field(max_length=50, description="City")
    postal_code: str = Field(max_length=10, description="Postal code")
    latitude: Optional[float] = Field(default=None, description="Latitude coordinate")
    longitude: Optional[float] = Field(default=None, description="Longitude coordinate")
    is_active: bool = Field(default=True, description="Whether the branch is active")


class Branch(BranchBase, TimestampMixin, table=True):
    """Branch table model."""

    __tablename__ = "branches"

    id: Optional[int] = Field(default=None, primary_key=True)

    # Relationship with footfall records
    footfall_records: List["Footfall"] = Relationship(back_populates="branch")


class BranchCreate(BranchBase):
    """Model for creating a new branch."""

    pass


class BranchRead(BranchBase):
    """Model for reading branch data."""

    id: int
    created_at: str
    updated_at: Optional[str] = None


class BranchUpdate(SQLModel):
    """Model for updating branch data."""

    name: Optional[str] = Field(default=None, max_length=100)
    address: Optional[str] = Field(default=None, max_length=200)
    city: Optional[str] = Field(default=None, max_length=50)
    postal_code: Optional[str] = Field(default=None, max_length=10)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    is_active: Optional[bool] = None
