import boto3
import pandas as pd
from io import BytesIO
from utils.etl_s3 import S3ApiETL


s3_client = boto3.client("s3")

sura_bucket = "sura-text-mining-poc"
target_prefix = "enriched/ranking-regions-complaints"

s3_helper = S3ApiETL(s3_client, sura_bucket, target_prefix)


def handler(_, __):
    df_source = get_source()

    df_result = apply_transformation(df_source)

    s3_helper.save_df(df_result)


def apply_transformation(df_source):
    complaints_tickets_df = df_source[['City', 'State', 'Status', 'Ticket #']]
    ticket_grouped_df = complaints_tickets_df.groupby(['City', 'State', 'Status'], as_index=False)
    ticket_grouped_df = ticket_grouped_df.count()

    return ticket_grouped_df


def get_source():
    bucket = "sura-text-mining-poc"

    key = "raw/complaints/complaints.csv"

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    obj = BytesIO(obj['Body'].read())

    complaints_df = pd.read_csv(obj)
    return complaints_df