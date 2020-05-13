import os
import boto3
import pandas as pd
import numpy as np

from io import BytesIO
from utils.etl_s3 import S3ApiETL               # pylint: disable=import-error

MAX_BATCH_LIMIT = 23
MAX_SENTENCE_LENGTH_IN_CHARS = 4500
DATALAKE_BUCKET = os.getenv('DATALAKE_BUCKET')
ENRICHED_PREFIX = os.getenv('ENRICHED_PREFIX')
RAW_PREFIX = os.getenv('RAW_PREFIX')

target_prefix = f'{ENRICHED_PREFIX}/text-quality-score-with-comprehend'

comprehend_client = boto3.client('comprehend')
s3_client = boto3.client("s3")
s3_helper = S3ApiETL(s3_client, DATALAKE_BUCKET, target_prefix)


def handler(_, __):
    df_source = S3ApiETL.get_object_as_dataframe(s3_client, DATALAKE_BUCKET, f'{RAW_PREFIX}/complaints.csv')
    df_result = apply_transformation(df_source)

    s3_helper.save_df(df_result)


def apply_transformation(df_source):
    df = df_source[["Ticket #", "Customer Complaint", "Description", "Status"]]

    opened_tickets_indexes = df["Status"] == "Open"
    df = df[opened_tickets_indexes]

    df["TextToBeAnalyzed"] = df["Description"].str[:MAX_SENTENCE_LENGTH_IN_CHARS]
    df["QualityScore"] = 0.0

    (rows, _) = df.shape
    splitted_dataframe = np.array_split(df, rows / MAX_BATCH_LIMIT)

    df_result = pd.DataFrame()

    for dataframe in splitted_dataframe:
        dataframe_selected = dataframe.reset_index()
        text_list = dataframe_selected["TextToBeAnalyzed"].tolist()

        response = comprehend_client.batch_detect_syntax(TextList=text_list, LanguageCode="en")
        dataframe_selected["QualityScore"] = calculate_score_from_comprehend_response(response)

        dataframe_selected = dataframe_selected[["Ticket #", "Customer Complaint", "QualityScore"]]
        df_result = pd.concat([df_result, dataframe_selected], ignore_index=True)

    df_result = df_result.sort_values(by=["QualityScore"], ascending=False)

    return df_result


def select_score_list(syntax_tokens):
    score_list = list(map(lambda r: r['PartOfSpeech']['Score'], syntax_tokens))
    return score_list


def calculate_score_from_comprehend_response(response):
    comprehend_result = pd.DataFrame(response["ResultList"])
    comprehend_result["QualityScore"] = comprehend_result["SyntaxTokens"].apply(select_score_list)
    comprehend_result["QualityScore"] = comprehend_result["QualityScore"].apply(lambda r: round(np.mean(r) * 100, 1))
    comprehend_result = comprehend_result["QualityScore"]

    return comprehend_result
