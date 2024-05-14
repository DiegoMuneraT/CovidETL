import pandas as pd
import numpy as np

import os
import boto3
import requests
import csv
import io

from datetime import datetime


def lambda_handler(event, context):
    
    file_name = event['file_name']
    
    # Create the amazon client
    s3 = boto3.client('s3')
    
    # Bucket name and its source
    source_bucket = 'rawapiuserdata'
    source_key = f'{file_name}'
    
    # Bucket name where we are going to store the transformed data
    destination_bucket = 'transformedapiuserdata'
    
    # Download the csv file from source
    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    csv_content = response['Body'].read().decode('utf-8')
    
    # Transform the data
    users = pd.read_csv(io.StringIO(csv_content))
    
    users_array = np.array(users[['date', 'positive', 'negative', 'hospitalizedCurrently']].iloc[0:100]).astype("int64")
    
    users_array_without_dates = (users_array[:, 1:]/100).astype("int64")
    
    total_per_row = np.sum(users_array_without_dates, axis=1)

    growth_rate = np.diff(total_per_row) / total_per_row[:-1] * 100
    
    growth_rate = np.insert(growth_rate, 0, 0)
    
    # Create the final data frame
    final_df = pd.DataFrame({'sourceDate': (users_array[:, 0]).astype(str), 'growthRate': (growth_rate).astype(float)})
    
    # Generate an unique file name
    now = datetime.now()
    date_now_str = now.strftime('%d-%m-%Y-%H-%M-%S')
    file_str = 'transformed_users_data_' + date_now_str + '.csv'
    
    # Save the data frame as csv in /tmp
    output_file_path = f'/tmp/{file_str}'
    final_df.to_csv(output_file_path, index=False)
    
    # Save the file into the s3 bucket
    s3.upload_file(output_file_path, destination_bucket, file_str)
    
    return {
        'body': 'File transformed and loaded succesfully',
        'output_file_path': output_file_path,
        'file_name': file_str
    }
