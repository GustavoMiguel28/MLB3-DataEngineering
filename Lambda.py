import json
import urllib.parse
import boto3

def lambda_handler(event, context):

    glue = boto3.client('glue')

    response = glue.start_job_run(
        JobName='DataB3-ETL'
    )

    return {
        'statusCode': 200,
        'body': 'Glue job iniciado com sucesso'
    }