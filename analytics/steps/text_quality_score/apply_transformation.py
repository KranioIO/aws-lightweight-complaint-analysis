import os
import boto3
import pandas as pd

from utils.etl_s3 import S3ApiETL                       # pylint: disable=import-error
from steps.text_quality_score.helpers import TextQualityAnalyzer as TQA    # pylint: disable=import-error
from steps.text_quality_score.helpers import words_list_from               # pylint: disable=import-error

DATALAKE_BUCKET = os.getenv('DATALAKE_BUCKET')
ENRICHED_PREFIX = os.getenv('ENRICHED_PREFIX')
RAW_PREFIX = os.getenv('RAW_PREFIX')
HUB_PREFIX = os.getenv('HUB_PREFIX')

target_prefix = f'{ENRICHED_PREFIX}/text-quality-score'

s3_client = boto3.client("s3")
s3_helper = S3ApiETL(s3_client, DATALAKE_BUCKET, target_prefix)


def handler(_, __):
    df_source = S3ApiETL.get_object_as_dataframe(s3_client, DATALAKE_BUCKET, f'{RAW_PREFIX}/complaints.csv')
    df_result = apply_transformation(df_source)

    s3_helper.save_df(df_result)


def apply_transformation(df_source):
    # # For multiple files:
    # matrix_words = files_to_lists() # Complete with all words files, separated by ','
    # tqa = TQA(*matrix_words)

    words_file = get_dictionary()

    # Save words from file in a list
    lst_words = words_list_from(words_file)
    tqa = TQA(lst_words)

    return execution_of_the_analysis(df_source, tqa)


def get_dictionary():
    obj = s3_client.get_object(Bucket=DATALAKE_BUCKET, Key=f'{HUB_PREFIX}/dictionary/english.0')
    dictionary = obj['Body'].read().decode('utf-8')

    return dictionary


def execution_of_the_analysis(df_source, tqa):
    df_result = df_source
    series_all_text = df_result["Customer Complaint"] + df_result["Description"]
    series_text_metrics_results = series_all_text.apply(tqa.generate_metrics_obj)

    df_result['Total Words'] = series_text_metrics_results.apply(lambda x: x.total_words)
    df_result['Wrong Words'] = series_text_metrics_results.apply(lambda x: x.wrong_words)
    df_result['Quality Score'] = series_text_metrics_results.apply(lambda x: x.quality_score)

    return df_result
