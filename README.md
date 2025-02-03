# Banking Agency Traffic Analyser
A project with :
- An API generating traffic count in bank agencies on an hourly basis
- An ETL pipeline which 
  - Extracts the data from the REST API (-> to raw CSVs))
  - Transform the data to perform analytical queries 
  - Load the data (-> to a parquet file)
- A Streamlit Analytical Dashboard to investigate data and metrics
