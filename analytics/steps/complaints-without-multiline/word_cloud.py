import boto3
import pandas as pd
import nltk
from nltk.corpus import stopwords

from io import BytesIO
from utils.etl_s3 import S3ApiETL

nltk.data.path.append("/tmp")
nltk.download('stopwords', download_dir='/tmp')
ESW = stopwords.words('english')

stop_words = list(ESW) + ["comcast", "xfinity", "i", "i'm", "don't", "xfintity", "am", "pm", "pt", "gb", "fcc", "br"]
punctuations_regex = r'(\'s|\W|\W+|[0-9_-])'
LIMIT_MOST_USED_WORDS = 500

s3_client = boto3.client("s3")

sura_bucket = "sura-text-mining-poc"
source_prefix = "enriched/complaints-standard"

target_prefix = "enriched/word-cloud"
s3_helper = S3ApiETL(s3_client, sura_bucket, target_prefix)


def handler(_, __):
    df_source = s3_helper.get_df_from_s3(sura_bucket, source_prefix)

    df_result = apply_transformation(df_source)

    s3_helper.save_df(df_result)


def apply_transformation(df_source):
    df_result = df_source.iloc[:1000].copy()
    df_result["WordCloud"] = generate_word_cloud(df_result["Description"])
    all_description_group_df = generate_most_used_words(df_result["WordCloud"], LIMIT_MOST_USED_WORDS)
    df_words_separated = separate_word_cloud(df_result)

    df_words_separated = df_words_separated.merge(all_description_group_df, on="Words")
    df_words_separated = df_words_separated[["Ticket #", "Sentiment", "Words"]]

    return df_words_separated


def generate_word_cloud(description):
    word_cloud = description.str.lower()

    word_cloud = word_cloud.str.replace(punctuations_regex, ' ', regex=True)
    word_cloud = word_cloud.str.split()
    word_cloud = word_cloud.apply(lambda w: [item for item in w if item])
    word_cloud = word_cloud.apply(lambda w: [item for item in w if item not in stop_words])
    word_cloud = word_cloud.str.join(" ")

    return word_cloud


def generate_most_used_words(word_cloud, limit):
    all_description = ' '.join(word_cloud)
    all_description_df = pd.DataFrame({"Words": all_description.split(), "WordCount": 1})

    all_description_group_df = all_description_df.groupby(["Words"], as_index=False).count()
    all_description_group_df = all_description_group_df[all_description_group_df["WordCount"] > 100]
    all_description_group_df = all_description_group_df.sort_values(by=["WordCount"], ascending=False)
    all_description_group_df = all_description_group_df.iloc[:limit]

    return all_description_group_df


def separate_word_cloud(df_result):
    df_tmp = df_result[["Ticket #", "WordCloud", "Sentiment"]].copy()

    df_words_separated = df_tmp.set_index("Ticket #").copy()
    df_words_separated["WordCloud"] = df_words_separated["WordCloud"].str.split()
    df_words_separated = df_words_separated["WordCloud"].apply(pd.Series)
    df_words_separated = df_words_separated.stack().to_frame()
    df_words_separated = df_words_separated.reset_index()
    df_words_separated = df_words_separated.rename(columns={0: "Words"})
    df_words_separated = df_words_separated[["Ticket #", "Words"]]
    df_words_separated = df_words_separated.merge(df_tmp, on="Ticket #")
    df_words_separated = df_words_separated[["Ticket #", "Sentiment", "Words"]]

    return df_words_separated
