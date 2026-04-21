import os
import pandas as pd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"
client = bigquery.Client()
df = pd.read_csv("data/online_retail.csv", encoding="ISO-8859-1")
df["InvoiceData"] = pd.to_datetime(df["InvoiceDate"])
table_id = "retail-analytics-494014.raw_data.online_retail_raw"
job = client.load_table_from_dataframe(df, table_id)
job.result() 

print("Upload complete!")