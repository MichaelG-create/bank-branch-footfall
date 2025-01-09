# Banking Agency Traffic Analyser
- An API generating traffic count in bank agencies on an hourly basis
- An ETL pipeline using pyspark which 
  - Extracts the data from the API and saves to CSV raw files (python)
  - Transforms the data to perform analytical queries (pyspark) 
  - Loads the data in parquet files (ideal for compression, performance and compatibility)
- A Grafana dashboard to present the analytical produced data
