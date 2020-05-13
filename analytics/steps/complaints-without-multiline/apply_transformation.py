import os
import boto3
import pandas as pd
import nltk

from nltk.corpus import stopwords
from io import BytesIO
from utils.etl_s3 import S3ApiETL               # pylint: disable=import-error

# from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS as ESW

nltk.data.path.append("/tmp")
nltk.download('stopwords', download_dir='/tmp')
ESW = stopwords.words('english')

DATALAKE_BUCKET = os.getenv('DATALAKE_BUCKET')
ENRICHED_PREFIX = os.getenv('ENRICHED_PREFIX')
RAW_PREFIX = os.getenv('RAW_PREFIX')

target_prefix = f'{ENRICHED_PREFIX}/complaints-without-multilines'

s3_client = boto3.client("s3")
s3_helper = S3ApiETL(s3_client, DATALAKE_BUCKET, target_prefix)


def handler(_, __):
    df_source = S3ApiETL.get_object_as_dataframe(s3_client, DATALAKE_BUCKET, f'{RAW_PREFIX}/complaints.csv')
    df_result = apply_transformation(df_source, ESW)

    s3_helper.save_df(df_result)


def apply_transformation(df_source, ESW):
    df_result = df_source.copy()
    generate_word_cloud(df_result, ESW)

    df_result["Description"] = df_result["Description"].str.replace(r'\n', '<br/>', regex=True)

    return df_result


def generate_word_cloud(df_result, ESW):
    stop_words = list(ESW) + ["comcast", "xfinity", "i", "i'm", "don't", "xfintity", "am", "pm", "pt", "gb"]

    df_result["WordCloud"] = df_result["Description"].str.lower()

    df_result["WordCloud"] = df_result["WordCloud"].str.replace(r'\'s|[\(\)\:\$\"\n\/,\.!?-]|[0-9]', ' ', regex=True)
    df_result["WordCloud"] = df_result["WordCloud"].str.split()
    df_result["WordCloud"] = df_result["WordCloud"].apply(lambda w: [item for item in w if item])
    df_result["WordCloud"] = df_result["WordCloud"].apply(lambda w: [item for item in w if item not in stop_words])
    df_result["WordCloud"] = df_result["WordCloud"].str.join(" ")
