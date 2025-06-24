"""Data extraction module for bank footfall data."""

import logging
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests
from pydantic import BaseModel, Field

# Import de la config (gère automatiquement le PYTHONPATH)
from bank_footfall.config import settings

# # Import de votre module existant
# try:
#     from date_randomseed import get_date_random_seed
# except ImportError:
#     # Fallback si le module n'est pas trouvé
#     sys.path.append(str(settings.project_root))


# Configuration du logging
logging.basicConfig(level=getattr(logging, settings.log_level))
logger = logging.getLogger(__name__)


class FootfallAPIResponse(BaseModel):
    """Model for API response validation."""

    branch_id: int = Field(..., description="Branch identifier")
    detector_id: str = Field(..., description="Detector identifier")
    timestamp: str = Field(..., description="Timestamp of measurement")
    visitor_count: int = Field(..., ge=0, description="Number of visitors")
    entry_count: int = Field(..., ge=0, description="Number of entries")
    exit_count: int = Field(..., ge=0, description="Number of exits")


class FootfallExtractor:
    """Extractor for bank footfall data."""

    def __init__(self, api_base_url: str = "http://127.0.0.1:8000"):
        self.api_base_url = api_base_url
        self.session = requests.Session()

    def extract_footfall_data(
        self, branch_id: int, detector_id: str
    ) -> List[Dict[str, Any]]:
        """Extract footfall data from API."""
        try:
            url = f"{self.api_base_url}/footfall"
            params = {"branch_id": branch_id, "detector_id": detector_id}

            logger.info(f"Extracting data from {url} with params {params}")
            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            # Validation avec Pydantic
            validated_data = [FootfallAPIResponse(**item).dict() for item in data]
            logger.info(f"Successfully extracted {len(validated_data)} records")

            return validated_data

        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Data extraction failed: {e}")
            raise

    def save_to_csv(self, data: List[Dict[str, Any]], output_path: Path) -> None:
        """Save extracted data to CSV."""
        try:
            df = pd.DataFrame(data)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            logger.info(f"Data saved to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            raise


def main():
    """Main extraction function - compatible avec votre code existant."""

    # Utilisation de la nouvelle config mais compatibilité avec l'existant
    extractor = FootfallExtractor()

    # Paramètres (vous pouvez les adapter selon vos besoins)
    branch_id = 1
    detector_id = "detector_001"

    # Extraction
    data = extractor.extract_footfall_data(branch_id, detector_id)

    # Sauvegarde (même répertoire que votre script existant)
    output_path = settings.project_root / "data" / "raw" / "footfall_data.csv"
    extractor.save_to_csv(data, output_path)

    logger.info("Extraction completed successfully")


if __name__ == "__main__":
    main()
