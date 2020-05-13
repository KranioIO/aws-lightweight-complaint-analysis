import os
import pandas as pd
import boto3
import re

from utils.etl_s3 import S3ApiETL           # pylint: disable=import-error

ENDPOINT_ARN = "arn:aws:comprehend:us-east-1:193024568733:document-classifier-endpoint/complaints-training-v2"
DATALAKE_BUCKET = os.getenv('DATALAKE_BUCKET')
ENRICHED_PREFIX = os.getenv('ENRICHED_PREFIX')
RAW_PREFIX = os.getenv('RAW_PREFIX')

s3_client = boto3.client("s3")
comprehend_client = boto3.client('comprehend')

target_prefix_lvl1 = "enriched/clustered-mails/0"
target_prefix_lvl2 = "enriched/clustered-mails/1100"

s3_helper_lvl1 = S3ApiETL(s3_client, DATALAKE_BUCKET, target_prefix_lvl1)
s3_helper_lvl2 = S3ApiETL(s3_client, DATALAKE_BUCKET, target_prefix_lvl2)


def handler_lvl1(_, __):
    df_source = S3ApiETL.get_object_as_dataframe(s3_client, DATALAKE_BUCKET, f'{RAW_PREFIX}/complaints.csv')
    df_result = apply_transformation(df_source, 0, 1100)

    s3_helper_lvl1.save_df(df_result)


def handler_lvl2(_, __):
    df_source = S3ApiETL.get_object_as_dataframe(s3_client, DATALAKE_BUCKET, f'{RAW_PREFIX}/complaints.csv')
    df_result = apply_transformation(df_source, 1100, 3000)

    s3_helper_lvl2.save_df(df_result)


def apply_transformation(df_source, start_index, end_index):
    df = df_source[['Ticket #', 'Customer Complaint', 'Description']].copy()

    df['TextToBeAnalyzed'] = df['Customer Complaint'].map(lambda x: re.sub(r'[,\.!?-]', '', x,))
    df['TextToBeAnalyzed'] = df['TextToBeAnalyzed'].map(lambda x: x.lower())

    df['Group Class'] = ''

    df = df.iloc[start_index:end_index].copy()

    df["Group Class"] = df["TextToBeAnalyzed"].apply(get_group_class)

    df_result = df[[
        "Ticket #", "Customer Complaint", "Group Class"
    ]]

    return df_result


def get_group_class(example_text):
    try:
        response = comprehend_client.classify_document(
            Text=example_text,
            EndpointArn=ENDPOINT_ARN
        )
    except Exception as e:
        print(e)
        return "General"

    classes = response["Classes"]
    group_class = max(classes, key=lambda x: x['Score'])
    group_class = group_class["Name"]

    return group_class
