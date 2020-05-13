import boto3
import pandas as pd
from io import BytesIO
from utils.etl_s3 import S3ApiETL
from functions.helpers import TextQualityAnalyzer as TQA
from functions.helpers import words_list_from


s3_client = boto3.client("s3")

sura_bucket = "sura-text-mining-poc"
target_prefix = "enriched/text-quality-score"



s3_helper = S3ApiETL(s3_client, sura_bucket, target_prefix)


def handler(_, __):
    df_source = get_source()

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


def get_source():
    bucket = "sura-text-mining-poc"
    key = 'raw/complaints/complaints.csv'

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    obj = BytesIO(obj['Body'].read())

    df_source = pd.read_csv(obj)

    return df_source


def get_dictionary():
    bucket = "sura-text-mining-poc"
    key = 'hub/dictionary/english.0'

    obj = s3_client.get_object(Bucket=bucket, Key=key)
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
