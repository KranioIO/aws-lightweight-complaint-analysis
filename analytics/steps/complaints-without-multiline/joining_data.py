import boto3
import pandas as pd

from io import BytesIO
from utils.etl_s3 import S3ApiETL

s3_client = boto3.client("s3")

sura_bucket = "sura-text-mining-poc"

target_prefix = "enriched/complaints-standard"
s3_helper = S3ApiETL(s3_client, sura_bucket, target_prefix)


def handler(_, __):
    df_source_list = get_source_list()

    df_result = apply_transformation(df_source_list)

    s3_helper.save_df(df_result)


def apply_transformation(df_source_list):
    df_result = df_source_list["source"].copy()
    df_age = df_source_list["age"]
    df_cluster = df_source_list["cluster"]
    df_sentiment = df_source_list["sentiment"]
    df_priority = df_source_list["priority"]

    df_age["Ticket #"] = df_age["Ticket #"].astype(str)
    df_age = df_age[["Ticket #", "age_in_days", "age_in_year_month_day"]]

    df_cluster["Ticket #"] = df_cluster["Ticket #"].astype(str)
    df_cluster = df_cluster[["Ticket #", "Group Class"]]

    df_sentiment["Ticket #"] = df_sentiment["Ticket #"].astype(str)
    df_sentiment = df_sentiment[["Ticket #", "Sentiment", "Positive", "Negative", "Neutral", "Mixed"]]

    df_priority["Ticket #"] = df_priority["Ticket #"].astype(str)
    df_priority = df_priority[["Ticket #", "Priority"]]

    df_result["Ticket #"] = df_result["Ticket #"].astype(str)
    df_result = df_result.merge(df_age, on="Ticket #", how="left")
    
    df_result["Ticket #"] = df_result["Ticket #"].astype(str)
    df_result = df_result.merge(df_cluster, on="Ticket #", how="left")
    
    df_result["Ticket #"] = df_result["Ticket #"].astype(str)
    df_result = df_result.merge(df_sentiment, on="Ticket #", how="left")
    
    df_result["Ticket #"] = df_result["Ticket #"].astype(str)
    df_result = df_result.merge(df_priority, on="Ticket #", how="left")

    df_result['Date'] = pd.to_datetime(df_result['Date'], infer_datetime_format=True)
    df_result['Date'] = df_result['Date'].dt.strftime('%Y-%m-%d')

    return df_result


def get_source_list():
    df_source_list = {}

    source_prefix = "enriched/complaints-without-multilines"
    df_source_list["source"] = s3_helper.get_df_from_s3(sura_bucket, source_prefix)

    age_prefix = "enriched/complaints-with-age"
    df_source_list["age"] = s3_helper.get_df_from_s3(sura_bucket, age_prefix)

    cluster_prefix = "enriched/clustered-mails"
    df_source_list["cluster"] = s3_helper.get_df_from_s3(sura_bucket, cluster_prefix)

    sentiment_prefix = "enriched/sentiment-analysis"
    df_source_list["sentiment"] = s3_helper.get_df_from_s3(sura_bucket, sentiment_prefix)

    priority_prefix = "enriched/complaints-priority"
    df_source_list["priority"] = s3_helper.get_df_from_s3(sura_bucket, priority_prefix)

    return df_source_list
