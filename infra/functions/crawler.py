import boto3
import os

crawler_name = os.getenv('CRAWLER')
client = boto3.client('glue')


def handler(_, __):
    response_3 = client.start_crawler(
        Name=crawler_name
    )

    return {
        "Status": 0
    }
