import os
import pandas_gbq
import pandas as pd
from google.cloud import bigquery
from utils.logger_config import get_logger
from utils.data_quality import run_data_checks

def ingest_data():
  try:

    logger = get_logger("main")

    logger.info("Starting ingestion...")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"
    client = bigquery.Client()

    df = pd.read_csv("data/online_retail.csv", encoding="ISO-8859-1")
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    pandas_gbq.to_gbq(dataframe=df, destination_table="bronze.online_retail_raw", project_id="retail-analytics-494014", if_exists="replace")
    logger.info(f"Successfully loaded {len(df)} rows.")

    logger.info("Starting silver layer transformation...")
    with open("sql/clean_data.sql", "r") as f:
      sql_silver = f.read()
    client.query(sql_silver).result()
    logger.info("Silver table created successfully.")

    bronze_count = client.query("SELECT COUNT(*) FROM `retail-analytics-494014.bronze.online_retail_raw`").result().to_dataframe().iloc[0,0]
    silver_count = client.query("SELECT COUNT(*) FROM `retail-analytics-494014.silver.online_retail_cleaned`").result().to_dataframe().iloc[0,0]
    dropped_rows = bronze_count - silver_count
    drop_percentage = round((dropped_rows / bronze_count) * 100, 2)
    logger.info(f"Data Quality Audit: Bronze ({bronze_count} rows) -> Silver ({silver_count} rows)")
    logger.info(f"Dropped {dropped_rows} invalid rows ({drop_percentage}% of total).")

    logger.info("Starting gold layer transformation...")
    with open("sql/insights.sql", "r") as f:
      sql_gold = f.read()
    client.query(sql_gold).result()
    logger.info("Gold table created successfully.")
    run_data_checks()

  except Exception as e:
    logger.error(f"Pipeline failed! Error details: {e}")
  finally:
    logger.info("Process finished.")


if __name__ == "__main__":
  ingest_data()