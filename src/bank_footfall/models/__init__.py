"""Database models for bank footfall tracking."""

from .base import Base
from .footfall import Footfall, FootfallCreate, FootfallRead, FootfallUpdate
from .branch import Branch, BranchCreate, BranchRead, BranchUpdate

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