# @Airflow
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
import boto3 # type: ignore

# @Python
from datetime import datetime, timedelta
import json

# URL where we are going to find historic covid values for US
_us_historic_covid_values_api_url = 'https://api.covidtracking.com/v1/us/daily.json?limit=10'

# Task for extracting data from API
def extract_data(ti):

    # Take the url from the variable
    url = _us_historic_covid_values_api_url

    # Instantiate the client for AWS Lambda
    lambda_client = boto3.client('lambda')

    # Define the payload
    payload = {
        'url': url
    }

    # Invoke lambda function
    response = lambda_client.invoke(
        FunctionName='extractData',
        InvocationType='RequestResponse', 
        Payload=json.dumps(payload)
    )

    # Read the response and store it
    result = json.loads(response['Payload'].read())

    # Send the file_name value of the response to the next task
    ti.xcom_push(key='response', value=result['file_name'])



# Task for transforming data stored in S3
def invoke_transform_lambda(ti):

    # Instantiate the client for AWS Lambda
    lambda_client = boto3.client('lambda')

    # Get the file_name from the previous task
    url = ti.xcom_pull(key='response', task_ids='extract_data')

    # Define the payload
    payload = {
        'file_name': url
    }

    # Invoke lambda function
    response = lambda_client.invoke(
        FunctionName='transformData',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )

# Definimos los argumentos por default para los dag

default_args = {
    'owner': 'Diego Munera',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    # 'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=60),
}

# Creamos los dag con los argumentos por default

with DAG(
        'covid_analytics_dag',
        default_args = default_args,
        # schedule_interval = '@weekly',
        catchup = False,     
        ) as dag:
    
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable= extract_data,
        dag = dag
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable= invoke_transform_lambda
    )

    extract_data >> transform_data
