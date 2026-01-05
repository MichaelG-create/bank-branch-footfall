"""Data extraction module for bank footfall data."""

import logging
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests
from pydantic import BaseModel

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
    agency_name: str
    date_time: str
    visitor_count: int
    unit: str
    counter_id: int | None = None


class FootfallExtractor:
    """Extractor for bank footfall data."""

    def __init__(self, api_base_url: str = "http://127.0.0.1:8000"):
        self.api_base_url = api_base_url
        self.session = requests.Session()

    def extract_footfall_data(
        self,
        date_time: str,
        agency_name: str,
        counter_id: int = -1,
        count_unit: str = "visitors",
    ) -> list[dict[str, Any]]:
        """Extract footfall data from API."""
        try:
            url = f"{self.api_base_url}/get_visitor_count"
            params = {
                "date_time": date_time,  # e.g. "2025-05-29 09:05"
                "agency_name": agency_name,  # e.g. "Aix_les_bains_1"
                "counter_id": counter_id,
                "count_unit": count_unit,
            }
            logger.info("Extracting data from %s with params %s", url, params)
            response = self.session.get(url, params=params)
            logger.info("Requested URL: %s", response.request.url)
            response.raise_for_status()

            data = response.json()  # <- single dict from FastAPI
            logger.info("Raw API response: %r", data)

            validated = FootfallAPIResponse(**data).model_dump()
            logger.info("Successfully extracted 1 record")

            return [validated]

        except requests.RequestException as e:
            logger.error("API request failed: %s", e)
            raise
        except Exception as e:
            logger.error("Data extraction failed: %s", e)
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


def main() -> None:
    extractor = FootfallExtractor()

    date_time = "2025-05-29 09:05"
    agency_name = "Aix_les_bains_1"
    counter_id = 0
    count_unit = "visitors"

    records = extractor.extract_footfall_data(
        date_time, agency_name, counter_id, count_unit
    )

    output_path = settings.project_root / "data" / "raw" / "footfall_data.csv"
    extractor.save_to_csv(records, output_path)
    logger.info("Extraction completed successfully")


if __name__ == "__main__":
    main()
