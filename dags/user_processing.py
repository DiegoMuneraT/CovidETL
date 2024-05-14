from airflow import DAG
from datetime import datetime
 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


import json
from pandas import json_normalize

# CONTROL K + C
# CONTROL K + U

def _transform_users():
    postgres_hook = PostgresHook(postgres_conn_id='postgres')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    select_query = "SELECT firstname, lastname FROM users;"
    cursor.execute(select_query)
    users = cursor.fetchall()

    transformed_users = [(f"{user[0]} {user[1]}", user[0], user[1]) for user in users]

    #Si quieres insertar en una nueva tabla:
    create_query = "CREATE TABLE IF NOT EXISTS transformed_users (full_name TEXT NOT NULL, firstname TEXT NOT NULL, lastname TEXT NOT NULL);"
    cursor.execute(create_query)

    insert_query = "INSERT INTO transformed_users (full_name, firstname, lastname) VALUES (%s, %s, %s);"
    cursor.executemany(insert_query, transformed_users)
    connection.commit()

    cursor.close()
    connection.close()



def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email'] })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)  

def _store_user():
    hook = PostgresHook(postgres_conn_id = 'postgres')
    hook.copy_expert(                                           #copy_expert works for making a copy of a file into our postgres db
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename= '/tmp/processed_user.csv'
    )


with DAG(
    dag_id="user_processing",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    create_table = PostgresOperator( 
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        ''')
        
 
    is_api_available = HttpSensor( #node=task, detects wheter de api to extract the data is good or not
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )
    
    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
    
    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user
    )
    
    store_user = PythonOperator(
        task_id = 'store_user',
        python_callable= _store_user
    )

    transform_users_task = PythonOperator(
    task_id='transform_users',
    python_callable=_transform_users,
    dag=dag,
    )

    create_table>>is_api_available>>extract_user >> process_user >> store_user >> transform_users_task
    
    # transform_users_task #process_user depends on extract_user