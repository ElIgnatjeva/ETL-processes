from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta 

with DAG('extract', 
         schedule=timedelta(minutes=30),
         start_date=datetime(2025, 1, 1),
         catchup=False,
         tags=['postgres', 'json']) as dag:
        history=SQLExecuteQueryOperator(
        task_id='extract_json',
        conn_id='demo',
        sql="""
        drop table if exists data_from_json;
        create table data_from_json as
        select
        post.value->>'name' as name, 
        post.value->>'species' as species, 
        post.value->>'favFoods' as favFoods, 
        post.value->>'birthYear' as birthYear,
        post.value->>'photo' as photo
        from json_content,
        jsonb_array_elements (json_data->'pets') as post (value);
        """
    )