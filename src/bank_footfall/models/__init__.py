"""Database models for bank footfall tracking."""

from .base import Base
from .branch import Branch, BranchCreate, BranchRead, BranchUpdate
from .footfall import Footfall, FootfallCreate, FootfallRead, FootfallUpdate

__all__ = [
    "Base",
    "Footfall",
    "FootfallCreate",
    "FootfallRead",
    "FootfallUpdate",
    "Branch",
    "BranchCreate",
    "BranchRead",
    "BranchUpdate",
]
