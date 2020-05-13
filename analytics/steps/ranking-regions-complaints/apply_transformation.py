import os
import boto3
import pandas as pd

from utils.etl_s3 import S3ApiETL               # pylint: disable=import-error


DATALAKE_BUCKET = os.getenv('DATALAKE_BUCKET')
ENRICHED_PREFIX = os.getenv('ENRICHED_PREFIX')
RAW_PREFIX = os.getenv('RAW_PREFIX')

target_prefix = f'{ENRICHED_PREFIX}/ranking-regions-complaints'

s3_client = boto3.client("s3")
s3_helper = S3ApiETL(s3_client, DATALAKE_BUCKET, target_prefix)


def handler(_, __):
    df_source = S3ApiETL.get_object_as_dataframe(s3_client, DATALAKE_BUCKET, f'{RAW_PREFIX}/complaints.csv')
    df_result = apply_transformation(df_source)

    s3_helper.save_df(df_result)


def apply_transformation(df_source):
    complaints_tickets_df = df_source[['City', 'State', 'Status', 'Ticket #']]
    ticket_grouped_df = complaints_tickets_df.groupby(['City', 'State', 'Status'], as_index=False)
    ticket_grouped_df = ticket_grouped_df.count()

    return ticket_grouped_df
