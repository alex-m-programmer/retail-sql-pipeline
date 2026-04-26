import os
from google.cloud import bigquery
from utils.logger_config import get_logger

def run_data_checks():  
  logger = get_logger("data_quality")
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"
  client = bigquery.Client()
  with open("sql/data_check.sql", "r") as f:
    data_check = f.read()
  
  try:
    logger.info("Running data quality checks on Gold table...")
    query_job = client.query(data_check)
    results = query_job.result()

    issue_count = 0
    for row in results:
      issue_count = row.issue_count

    if issue_count > 0:
      logger.critical(f"DATA BREACH! Found {issue_count} invalid rows. Stopping pipeline.")
      raise ValueError("Data quality check failed.")
    else:
      logger.info("Data quality checks passed! (0 issues found)")

  except Exception as e:
    logger.error(f"Quality Check Error: {e}")
    raise e
