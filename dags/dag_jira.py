
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.http.sensors.http import HttpSensor

import os
from io import StringIO
import io
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy

import json
import requests
from jira import JIRA
import boto3

headers ={
    "Accept": "application/json",
    "Content/Type": "application/json"
}
auth_jira = JIRA('https://bidev.atlassian.net', basic_auth=('minh.grace95@gmail.com', 'ATATT3xFfGF0enP-hFsCK-vJMAXyhOmItUXFWx_wF9QLrYeBWRvki9ArwqCHR1ZOEJYJBfrLADIqxWugP0NS8-yPqTPmxnYEQgCtDQ4C2oj6i7T0dgz80Vp1mUovFhZrL34d3gb3PqmUQGTghM5i7AVHQ_AwH0_LOw-MP1Zed87gv6XIece6ZlI=F64DC9A7'))
#api_url = f"{url}?jql=project={project_key}"
url ="https://bidev.atlassian.net/rest/api/3/search"
query = {
    "jql" : "project = BID"
}

response=requests.get(url, params=query,  auth=('minh.grace95@gmail.com', 'ATATT3xFfGF0enP-hFsCK-vJMAXyhOmItUXFWx_wF9QLrYeBWRvki9ArwqCHR1ZOEJYJBfrLADIqxWugP0NS8-yPqTPmxnYEQgCtDQ4C2oj6i7T0dgz80Vp1mUovFhZrL34d3gb3PqmUQGTghM5i7AVHQ_AwH0_LOw-MP1Zed87gv6XIece6ZlI=F64DC9A7'))
#response=response.json()
  # response=pd.json_normalize(response)
response = response.json()



for issue in response:
    data = []
    data = pd.json_normalize(data)
    issue = response['issues']
   # df = df.append(issue)
    data['key'] = [d['key'] for d in issue]
    data['changelog'] =  [d['fields'] for d in issue]
    data['changelog'] = data['changelog'].astype('string')


# load data to postgres
access_key = ''
secret_access_key =''

def load():

    # save to s3
    upload_file_bucket = 'jira-pipeline-bucket'
    upload_file_key = 'public/' 
    filepath =  upload_file_key + ".csv"
    #
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,region_name='us-east-1')
    with io.StringIO() as csv_buffer:
        data.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )


default_args ={
    'owner': 'Phuong',
    'retries':5,
    'retry_delay' : timedelta(minutes=5),
    'provide_context':True
}
with DAG(
    dag_id= 'jira_data_dag',
    default_args=default_args,
    description='the 2nd test dag',
    start_date= datetime(2024,11,9),
    render_template_as_native_obj=True
) as dag:
    
    load_jira_data =PythonOperator(
        task_id='load_jira_data',
        python_callable=load
    )

