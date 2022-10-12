import boto3
import time
import io
import os

def lambda_handler(event,context):
    table_name = event['pathParameters']['table_name']
    catalog_name = 'sitcen_ingest'
    database_name ='sitcenharmonised'
    # schema_name = 'ons'
    response_root = f'{catalog_name}.{database_name}.{table_name}'
    client = boto3.client('athena')
    query_string = f'select * from {response_root} limit 10'
    output_dir = 's3://all-'+os.env('S3_OUTPUT')+'-tenant-bucket/SitCen/temp'
    query_id = client.start_query_execution(
            QueryString = query_string,
            QueryExecutionContext = {
                'Database': database_name,
                'Catalog': catalog_name
            },
            ResultConfiguration = {
                'OutputLocation': output_dir
            }
        )['QueryExecutionId']

    query_execution_id = None
    execution_query_id = client.get_query_execution(QueryExecutionId=query_id)
    query_execution_id = execution_query_id['QueryExecution']['QueryExecutionId']
    queryResponse = {}
    queryResponse["QueryExecutionId"] =query_execution_id 


    #3. Construct http response object
    responseObject = {}
    responseObject['statusCode'] = 200
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json '
    responseObject['body'] = queryResponse

    #4. Return the response object
    return responseObject