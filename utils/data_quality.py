import os
from dotenv import load_dotenv
from google.cloud import bigquery
from utils.logger_config import get_logger

load_dotenv()

def run_data_checks():  
  logger = get_logger("data_quality")
  client = bigquery.Client()
    
  with open("sql/data_check.sql", "r") as f:
    template = f.read()
    
  try:
    logger.info("Running data quality checks on Gold table...")
    formatted_sql = template.format(
      PROJECT_ID = os.getenv("PROJECT_ID"),
      DATASET_GOLD = os.getenv("DATASET_GOLD"),
      TABLE_PERFORMANCE = os.getenv("TABLE_PERFORMANCE")
    )

    query_job = client.query(formatted_sql)
    results = query_job.result()

    row = list(results)[0]
    issue_count = row.issue_count
    issue_types = row.issue_types

    if issue_count > 0:
      logger.critical(f"QUALITY GATE FAILED! Issues: {issue_types}")
      raise ValueError(f"Data quality check failed: {issue_types}")
        
    logger.info("Data quality checks passed! (0 issues found)")

  except Exception as e:
    logger.error(f"Quality Check Error: {e}")
    raise e