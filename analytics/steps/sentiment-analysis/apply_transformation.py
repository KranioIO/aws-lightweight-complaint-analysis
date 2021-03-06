import os
import boto3
import pandas as pd
import numpy as np

from utils.etl_s3 import S3ApiETL               # pylint: disable=import-error

s3_client = boto3.client("s3")
comprehend_client = boto3.client('comprehend')

MAX_BATCH_LIMIT = 23
MAX_SENTENCE_LENGTH_IN_CHARS = 4500
DATALAKE_BUCKET = os.getenv('DATALAKE_BUCKET')
ENRICHED_PREFIX = os.getenv('ENRICHED_PREFIX')
RAW_PREFIX = os.getenv('RAW_PREFIX')

target_prefix = f'{ENRICHED_PREFIX}/sentiment-analysis'
target_prefix_sentiments = f'{ENRICHED_PREFIX}/tickets-by-sentiments'
target_prefix_city_state_sentiments = f'{ENRICHED_PREFIX}/tickets-by-city-state-sentiments'

s3_helper = S3ApiETL(s3_client, DATALAKE_BUCKET, target_prefix)
s3_helper_sentiments = S3ApiETL(s3_client, DATALAKE_BUCKET, target_prefix_sentiments)
s3_helper_city_state_sentiments = S3ApiETL(s3_client, DATALAKE_BUCKET, target_prefix_city_state_sentiments)


def handler(_, __):
    df_source = S3ApiETL.get_object_as_dataframe(s3_client, DATALAKE_BUCKET, f'{RAW_PREFIX}/complaints.csv')
    df_result, df_result_group_sentiments, df_group_city_state_sentiments = apply_transformation(df_source)

    s3_helper.save_df(df_result)
    s3_helper_sentiments.save_df(df_result_group_sentiments)
    s3_helper_city_state_sentiments.save_df(df_group_city_state_sentiments)


def apply_transformation(df_source):
    df = df_source[["Ticket #", "Customer Complaint", "City", "State", "Status", "Description"]].copy()

    df['Sentiment'] = ''
    df['Positive'] = 0.0
    df['Negative'] = 0.0
    df['Neutral'] = 0.0
    df['Mixed'] = 0.0

    df["TextToBeAnalyzed"] = df["Description"].str[:MAX_SENTENCE_LENGTH_IN_CHARS]

    (rows, _) = df.shape
    splitted_dataframe = np.array_split(df, rows / MAX_BATCH_LIMIT)

    df_result = pd.DataFrame()

    for dataframe in splitted_dataframe:
        dataframe_selected = dataframe.reset_index().copy()
        text_list = dataframe_selected["TextToBeAnalyzed"].tolist()

        comprehend_result = get_sentiment_analysis_batch(text_list)

        dataframe_selected['Sentiment'] = comprehend_result['Sentiment']
        dataframe_selected['Positive'] = comprehend_result['Positive']
        dataframe_selected['Negative'] = comprehend_result['Negative']
        dataframe_selected['Neutral'] = comprehend_result['Neutral']
        dataframe_selected['Mixed'] = comprehend_result['Mixed']

        df_result = pd.concat([df_result, dataframe_selected], ignore_index=True)

    df_result = df_result[[
        "Ticket #",
        "Customer Complaint",
        "City",
        "State",
        "Status",
        "Description",
        "Sentiment",
        "Negative",
        "Positive",
        "Neutral",
        "Mixed"
    ]]

    df_result_group_sentiments = df_result[["Ticket #", "Sentiment"]].groupby(['Sentiment'], as_index=False).count()

    df_group_city_state_sentiments = df_result[["Ticket #", "City", "State", "Sentiment"]]
    group_columns = ['City', 'State', 'Sentiment']

    df_group_city_state_sentiments = df_group_city_state_sentiments.groupby(group_columns, as_index=False)
    df_group_city_state_sentiments = df_group_city_state_sentiments.count()

    return df_result, df_result_group_sentiments, df_group_city_state_sentiments


def get_sentiment_analysis_batch(text_list):
    response = comprehend_client.batch_detect_sentiment(TextList=text_list, LanguageCode='en')
    comprehend_result = pd.DataFrame(response["ResultList"])
    sentiment_score = comprehend_result["SentimentScore"].apply(pd.Series)
    comprehend_result = comprehend_result[["Sentiment"]].merge(sentiment_score, left_index=True, right_index=True)

    return comprehend_result
