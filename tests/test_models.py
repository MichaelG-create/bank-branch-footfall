from datetime import datetime

from bank_footfall.models.branch import Branch
from bank_footfall.models.footfall import Footfall


def test_branch_model_basic():
    branch = Branch(name="Main", address="123 Main St", city="Geneva", postal_code="1200")
    assert branch.name == "Main"
    assert branch.city == "Geneva"
    assert branch.address == "123 Main St"
    assert branch.postal_code == "1200"
    assert branch.is_active is True


def test_footfall_model_basic():
    footfall = Footfall(
        branch_id=1,
        detector_id="DETECTOR-001",
        timestamp=datetime(2025, 1, 1, 10, 0, 0),
        visitor_count=10,
        entry_count=5,
        exit_count=3,
    )
    assert footfall.branch_id == 1
    assert footfall.detector_id == "DETECTOR-001"
    assert footfall.visitor_count == 10
    assert footfall.entry_count == 5
    assert footfall.exit_count == 3