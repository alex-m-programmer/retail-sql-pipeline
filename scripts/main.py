import os
import pandas as pd
from google.cloud import bigquery
from utils.logger_config import get_logger



def ingest_data():
  try:

    logger = get_logger("main")

    logger.info("Starting ingestion...")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"
    client = bigquery.Client()
    df = pd.read_csv("data/online_retail.csv", encoding="ISO-8859-1")
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    table_id = "retail-analytics-494014.bronze.online_retail_raw"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    logger.info(f"Successfully loaded {len(df)} rows.")

    logger.info("Starting silver layer transformation...")
    with open("sql/clean_data.sql", "r") as f:
      sql_silver = f.read()
    client.query(sql_silver).result()
    logger.info("Silver table created successfully.")

    logger.info("Starting gold layer transformation...")
    with open("sql/insights.sql", "r") as f:
      sql_gold = f.read()
    client.query(sql_gold).result()
    logger.info("Gold table created successfully.")

  except Exception as e:
    logger.error(f"Pipeline failed! Error details: {e}")
  finally:
    logger.info("Process finished.")



if __name__ == "__main__":
  ingest_data()
  
##################### CONSIDER CHANGING VISUALIAZATION TO POWER BI