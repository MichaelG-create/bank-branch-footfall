"""Streamlit APP displaying sensors traffic temporal series"""

# pylint: disable=C0301
# ┌────────────┬─────────────────┬────────────┬──────────┬─────────────────────┬─────────┬──────────────────────┬───────────────────┬────────────┐
# │    date    │   agency_name   │ counter_id │   unit   │ daily_visitor_count │ weekday │ avg_visits_4_weekday │ prev_avg_4_visits │ pct_change │
# │    date    │     varchar     │   int32    │ varchar  │        int64        │  int32  │        double        │      double       │   double   │
# ├────────────┼─────────────────┼────────────┼──────────┼─────────────────────┼─────────┼──────────────────────┼───────────────────┼────────────┤
# │ 2024-05-12 │ aix_les_bains_1 │          0 │ visitors │                 100 │       1 │                100.0 │              NULL │       NULL │
# └────────────┴─────────────────┴────────────┴──────────┴─────────────────────┴─────────┴──────────────────────┴───────────────────┴────────────┘

import duckdb

# Read the parquet file directly in duckdb (memory costless)
PARQUET_FILE = (
    "'data/filtered/2024_agencies_daily_visitor_count/"
    "part-00000-0e27bd92-4b26-465a-9cf9-180b61966469-c000.snappy.parquet'"
)


# pylint: disable=C0303
QUERY = f"""
SELECT * 
  FROM {PARQUET_FILE} 
  LIMIT 2
"""

duckdb.sql(QUERY).show()
