import pandas as pd
import os
import boto3
from datetime import datetime
import requests

def lambda_handler(event, context):
    # Suponiendo que la URL se pasa como parte del evento de Lambda
    url = event['url']

    # Lee el archivo desde la URL
    response = requests.get(url)

    # Verifica si la API respondió correctamente
    if response.status_code == 200:
        # Convierte la respuesta a un DataFrame
        df = pd.DataFrame(response.json())

        # Genera un nombre de archivo único
        now = datetime.now()
        date_now_str = now.strftime('%d-%m-%Y-%H-%M-%S')
        file_str = 'raw_users_data_' + date_now_str + '.csv'

        # Guarda el DataFrame como CSV en /tmp
        output_file_path = f'/tmp/{file_str}'
        df.to_csv(output_file_path, index=False)

        # Sube el archivo a un bucket de S3
        s3 = boto3.client('s3')
        bucket_name = 'rawapiuserdata'
        s3.upload_file(output_file_path, bucket_name, file_str)

        # Retorna el path del archivo
        return {
            'output_file_path': output_file_path,
            'file_name': file_str
        }
    else:
        return {
            'error': 'API did not respond correctly'
        }
