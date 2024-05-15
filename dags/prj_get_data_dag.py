from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum
import boto3
import vertica_python

conn_info = {
        'host': '51.250.75.20',
        'port': 5433,
        'user': 'stv2024031225',
        'password': 'g1q28KFRucwCOut',
        'ssl': False,
        'autocommit': True,
        'connection_timeout': 5
}

def fetch_s3_files(bucket: str, keys: List[str]) -> None:
    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    for key in keys:
        s3_client.download_file(
            Bucket=bucket,
            Key=key,
            Filename=f'/data/{key}'
        )

def load_to_stg(table: str, filename: str) -> None:
    with vertica_python.connect(**conn_info) as connection:
        cur = connection.cursor()
        cur.execute(
            f"""
            COPY stv2024031225__STAGING.{table}
            FROM LOCAL '/data/{filename}'
            ENCLOSED BY '"'
            DELIMITER ','
            REJECTED DATA AS TABLE {table}_rej
            SKIP 1
            """
        )

@dag(schedule_interval=None, start_date=pendulum.parse('2023-05-11'))
def project6_dag():
    bucket_files = ['group_log.csv']
    get_data = PythonOperator(
        task_id='fetch_files',
        python_callable=fetch_s3_files,
        op_kwargs={'bucket': 'sprint6', 'keys': bucket_files},
    )

    load_data  = PythonOperator(
        task_id='load_groups_log_to_stg',
        python_callable=load_to_stg,
        op_kwargs={'table': 'group_log', 'filename': bucket_files[0]}
    )

    get_data >> load_data

_ = project6_dag()