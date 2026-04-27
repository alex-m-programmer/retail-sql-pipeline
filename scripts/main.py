import os
import pandas_gbq
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from utils.logger_config import get_logger
from utils.data_quality import run_data_checks

load_dotenv()

def ingest_data():
  logger = get_logger("main")
  try:
    logger.info("Starting ingestion...")
        
    project_id = os.getenv("PROJECT_ID")
    client = bigquery.Client(project=project_id)

    df = pd.read_csv("data/online_retail.csv", encoding="ISO-8859-1")
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
        
    destination = f"{os.getenv('DATASET_BRONZE')}.{os.getenv('TABLE_RAW')}"
    pandas_gbq.to_gbq(df, destination, project_id=project_id, if_exists="replace")
    logger.info(f"Successfully loaded {len(df)} rows to Bronze.")

    logger.info("Starting silver layer transformation...")
    with open("sql/clean_data.sql", "r") as f:
      sql_silver = f.read().format(
        PROJECT_ID = os.getenv("PROJECT_ID"),
        DATASET_SILVER = os.getenv("DATASET_SILVER"),
        TABLE_CLEANED = os.getenv("TABLE_CLEANED"),
        DATASET_BRONZE = os.getenv("DATASET_BRONZE"),
        TABLE_RAW = os.getenv("TABLE_RAW")
      )

    client.query(sql_silver).result()
    logger.info("Silver table created successfully.")

    bronze_query = f"SELECT COUNT(*) FROM `{project_id}.{os.getenv('DATASET_BRONZE')}.{os.getenv('TABLE_RAW')}`"
    silver_query = f"SELECT COUNT(*) FROM `{project_id}.{os.getenv('DATASET_SILVER')}.{os.getenv('TABLE_CLEANED')}`"
        
    bronze_count = client.query(bronze_query).result().to_dataframe().iloc[0,0]
    silver_count = client.query(silver_query).result().to_dataframe().iloc[0,0]
        
    dropped_rows = bronze_count - silver_count
    logger.info(f"Audit: Bronze ({bronze_count}) -> Silver ({silver_count}). Dropped: {dropped_rows}")

    logger.info("Starting gold layer transformation...")
    with open("sql/insights.sql", "r") as f:
      sql_gold = f.read().format(
        PROJECT_ID = os.getenv("PROJECT_ID"),
        DATASET_GOLD = os.getenv("DATASET_GOLD"),
        TABLE_PERFORMANCE = os.getenv("TABLE_PERFORMANCE"),
        DATASET_SILVER = os.getenv("DATASET_SILVER"),
        TABLE_CLEANED = os.getenv("TABLE_CLEANED")
      )
    client.query(sql_gold).result()
       
    run_data_checks() 
    logger.info("Pipeline completed successfully.")

  except Exception as e:
    logger.error(f"Pipeline failed! Details: {e}")
  finally:
    logger.info("Process finished.")

if __name__ == "__main__":
  ingest_data()