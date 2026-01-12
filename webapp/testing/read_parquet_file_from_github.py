"""Streamlit APP displaying sensors traffic temporal series"""

# pylint: disable=C0301
# ┌────────────┬─────────────────┬────────────┬──────────┬─────────────────────┬─────────┬──────────────────────┬───────────────────┬────────────┐
# │    date    │   agency_name   │ counter_id │   unit   │ daily_visitor_count │ weekday │ avg_visits_4_weekday │ prev_avg_4_visits │ pct_change │
# │    date    │     varchar     │   int32    │ varchar  │        int64        │  int32  │        double        │      double       │   double   │
# ├────────────┼─────────────────┼────────────┼──────────┼─────────────────────┼─────────┼──────────────────────┼───────────────────┼────────────┤
# │ 2024-05-12 │ aix_les_bains_1 │          0 │ visitors │                 100 │       1 │                100.0 │              NULL │       NULL │
# └────────────┴─────────────────┴────────────┴──────────┴─────────────────────┴─────────┴──────────────────────┴───────────────────┴────────────┘


import pandas as pd
import requests

parquet_url = (
    "https://raw.githubusercontent.com/michaelg-create/bank-branch-footfall/main/"
    "data/filtered/parquet/part-00000-521db14e-a1b8-48eb-8570-4e6af31e6c16-c000.snappy.parquet"
)

parquet_file = "part-00000-521db14e-a1b8-48eb-8570-4e6af31e6c16-c000.snappy.parquet"

# Télécharger le fichier
print(f"Téléchargement depuis : {parquet_url}")

response = requests.get(parquet_url, headers={"User-Agent": "Mozilla/5.0"})
# Vérifier si le téléchargement est correct
if response.status_code == 200:
    with open(parquet_file, "wb") as f:
        f.write(response.content)

        # Vérifier la taille du fichier téléchargé
        print(
            f"Fichier téléchargé : {parquet_file}, Taille : {len(response.content)} octets"
        )

        # Charger le fichier Parquet avec Pandas
        df = pd.read_parquet(parquet_file)
        print(df.head())
else:
    print(f"Erreur {response.status_code} lors du téléchargement.")


df.show()

# # Read the parquet file directly in duckdb (memory costless)
# PARQUET_FILE = (
#     "'data/filtered/2024_agencies_daily_visitor_count/"
#     "part-00000-0e27bd92-4b26-465a-9cf9-180b61966469-c000.snappy.parquet'"
# )
#
#
# # pylint: disable=C0303
# QUERY = f"""
# SELECT *
#   FROM {PARQUET_FILE}
#   LIMIT 2
# """
#
# duckdb.sql(QUERY).show()
