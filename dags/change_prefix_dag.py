from datetime import datetime
from typing import Final

import boto3
from airflow.decorators import *
from airflow.models import Variable

service_name: Final[str] = 's3'
endpoint_url: Final[str] = 'https://kr.object.ncloudstorage.com'
access_key: str = Variable.get('access_key')
secret_key: str = Variable.get('secret_key')
nic_id: int = int(Variable.get('nic_id'))

bucket: str = Variable.get('bucket')
source_prefix = f'VPC_FLOW_LOG/1_{nic_id}_'
compression_prefix = 'compression_target'

s3 = boto3.client(service_name, endpoint_url=endpoint_url, aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key)


@dag(schedule=None, start_date=datetime(year=2023, month=12, day=1, hour=0), catchup=False)
def move_file_dag():
    @task
    def delete_old_file():
        objects = s3.list_objects(Bucket=bucket, Prefix=compression_prefix, )

        for o in objects['Contents']:
            s3.delete_object(Bucket=bucket, Key=o['Key'])

    @task()
    def file_move_task():
        """
        압축 대상 파일들을 data flow 에서 읽어갈 수 있게 path 를 변경
        """

        objects = s3.list_objects(Bucket=bucket, Prefix=source_prefix, )

        for o in objects['Contents']:
            source_file_path: str = o['Key']
            source_file_name: str = source_file_path.split("/")[-1]

            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': source_file_path},
                           Key=f'{compression_prefix}/{source_file_name}')

            # Delete the original object
            s3.delete_object(Bucket=bucket, Key=source_file_path)


move_file_dag()
