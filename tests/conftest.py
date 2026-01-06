# tests/conftest.py
import sys
from pathlib import Path

from bank_footfall.config.config import settings

PROJECT_ROOT: Path = settings.project_root
WEBAPP_DIR = PROJECT_ROOT / "webapp"

# Ensure webapp/ is on sys.path so `import app` works everywhere
if str(WEBAPP_DIR) not in sys.path:
    sys.path.insert(0, str(WEBAPP_DIR))
