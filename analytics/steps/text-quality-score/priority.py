import boto3
import pandas as pd

from io import BytesIO
from utils.etl_s3 import S3ApiETL
from datetime import datetime

DAYS_IN_MONTH = 30
LAST_DATE = datetime(2015, 7, 1)

s3_client = boto3.client("s3")

sura_bucket = "sura-text-mining-poc"
source_prefix = "enriched/text-quality-score-with-comprehend"

target_prefix = "enriched/complaints-priority"
s3_helper = S3ApiETL(s3_client, sura_bucket, target_prefix)


def handler(_, __):
    df_source = s3_helper.get_df_from_s3(sura_bucket, source_prefix)

    df_result = apply_transformation(df_source)

    s3_helper.save_df(df_result)


def apply_transformation(df_source):
    df_result = df_source.copy()
    df_result["Priority"] = df_result["QualityScore"].apply(lambda t: "High" if t > 98 else "Medium" if t > 96 else "Low")

    return df_result
