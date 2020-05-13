import boto3
import pandas as pd

# from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS as ESW

import nltk
from nltk.corpus import stopwords

from io import BytesIO
from utils.etl_s3 import S3ApiETL

nltk.data.path.append("/tmp")
nltk.download('stopwords', download_dir='/tmp')
ESW = stopwords.words('english')

s3_client = boto3.client("s3")

sura_bucket = "sura-text-mining-poc"

target_prefix = "enriched/complaints-without-multilines"
s3_helper = S3ApiETL(s3_client, sura_bucket, target_prefix)


def handler(_, __):
    df_source = get_source()

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

def get_source():
    bucket = "sura-text-mining-poc"

    key = "raw/complaints/complaints.csv"

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    obj = BytesIO(obj['Body'].read())

    complaints_df = pd.read_csv(obj)
    return complaints_df
