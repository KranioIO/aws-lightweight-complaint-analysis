import boto3
import time

client = boto3.client('comprehend')


def create(_, __):
    response = client.create_endpoint(
        EndpointName='complaints-training-v2',
        ModelArn='arn:aws:comprehend:us-east-1:193024568733:document-classifier/complaints-training-v2',
        DesiredInferenceUnits=1,
        Tags=[
            {
                'Key': 'Project',
                'Value': 'K-Storm'
            },
            {
                'Key': 'Client',
                'Value': 'Sura'
            },
        ]
    )


    while True:
        response_2 = client.describe_endpoint(
            EndpointArn=response['EndpointArn']
        )

        print(response_2['EndpointProperties']['Status'])

        if response_2['EndpointProperties']['Status'] != 'CREATING':
            break

        time.sleep(60)

    return {
        "Status": 0
    }


def delete(_, __):
    endpoint_arn = "arn:aws:comprehend:us-east-1:193024568733:document-classifier-endpoint/complaints-training-v2"

    response_3 = client.delete_endpoint(
        EndpointArn=endpoint_arn
    )

    return {
        "Status": 0
    }
