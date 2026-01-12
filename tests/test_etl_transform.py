import duckdb
from bank_footfall.etl.transform_load import run_pipeline
from pyspark.sql import SparkSession


def test_run_pipeline_creates_parquet(tmp_path, monkeypatch):
    project_root = tmp_path

    raw_dir = project_root / "data" / "raw"
    raw_dir.mkdir(parents=True)

    db_dir = project_root / "data" / "data_base"
    db_dir.mkdir(parents=True)
    db_path = db_dir / "agencies.duckdb"

    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE agencies (agency_name TEXT)")
    con.execute("INSERT INTO agencies VALUES ('main branch')")
    con.close()

    (raw_dir / "footfall.csv").write_text(
        "date_time,agency_name,counter_id,visitor_count,unit\n"
        "2025-01-01 10:00,Main Branch,1,10,visitors\n"
    )

    spark = SparkSession.builder.appName("test_pipeline").getOrCreate()

    try:
        run_pipeline(project_root=project_root, spark=spark)

        output_dir = project_root / "data" / "filtered" / "parquet"
        assert output_dir.exists()

        con = duckdb.connect()

        # 1) Either use glob:
        df = con.execute(f"SELECT * FROM read_parquet('{output_dir}/*.parquet')").df()

        # or, if Spark writes a single file, you can also:
        # df = con.execute(
        #     f\"SELECT * FROM '{output_dir}/part-*.parquet'\"
        # ).df()

        con.close()

        assert not df.empty
        assert set(df.columns) >= {
            "date",
            "agency_name",
            "counter_id",
            "daily_visitor_count",
        }
    finally:
        spark.stop()
