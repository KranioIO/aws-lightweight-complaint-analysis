import boto3
import pandas as pd

from io import BytesIO
from utils.etl_s3 import S3ApiETL
from datetime import datetime

DAYS_IN_MONTH = 30
LAST_DATE = datetime(2015, 7, 1)

s3_client = boto3.client("s3")

sura_bucket = "sura-text-mining-poc"

target_prefix_age = "enriched/complaints-with-age"
s3_helper_age = S3ApiETL(s3_client, sura_bucket, target_prefix_age)

target_prefix_group = "enriched/complaints-count-by-age"
s3_helper_group = S3ApiETL(s3_client, sura_bucket, target_prefix_group)


def handler(_, __):
    df_source = get_source()

    df_result_age, df_result_group = apply_transformation(df_source)

    s3_helper_age.save_df(df_result_age)
    s3_helper_group.save_df(df_result_group)


def apply_transformation(df_source):
    complaints_tickets_df = df_source[["Ticket #", "Customer Complaint", "Date", "Time", "Status"]]

    complaints_tickets_df['Date'] = pd.to_datetime(complaints_tickets_df['Date'], infer_datetime_format=True)

    complaints_tickets_df = complaints_tickets_df[complaints_tickets_df["Status"] == "Open"]

    df_age = LAST_DATE - complaints_tickets_df['Date']
    complaints_tickets_df['age_in_days'] = df_age.dt.days
    complaints_tickets_df['Date'] = complaints_tickets_df['Date'].dt.strftime('%Y-%m-%d')

    complaints_tickets_df['age_in_year_month_day'] = complaints_tickets_df['age_in_days'].apply(convert_age_in_days)

    complaints_tickets_df = complaints_tickets_df.sort_values(by=['age_in_days'], ascending=False)

    # groupby age and count
    grouped_tickets_by_date = complaints_tickets_df[["Date", "Ticket #"]].groupby(["Date"], as_index=False)
    grouped_tickets_by_date = grouped_tickets_by_date.count()
    grouped_tickets_by_date = grouped_tickets_by_date.sort_values(by=['Date'], ascending=False)
    grouped_tickets_by_date = grouped_tickets_by_date.rename(columns={"Ticket #": "ticket_count"})

    return complaints_tickets_df, grouped_tickets_by_date


def convert_age_in_days(age_in_days):
    year = int(age_in_days / 365)
    month = int((age_in_days % 365) / DAYS_IN_MONTH)
    days = (age_in_days % 365) % DAYS_IN_MONTH

    age_in_year_month_day = []

    if year:
        age_in_year_month_day.append("%s years" % year)

    if month:
        age_in_year_month_day.append("%s months" % month)

    if days:
        age_in_year_month_day.append("%s days" % days)

    age_in_year_month_day = " ".join(age_in_year_month_day)
    return age_in_year_month_day


def get_source():
    bucket = "sura-text-mining-poc"

    key = "raw/complaints/complaints.csv"

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    obj = BytesIO(obj['Body'].read())

    complaints_df = pd.read_csv(obj)
    return complaints_df
