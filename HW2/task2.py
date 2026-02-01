from airflow import DAG
from datetime import datetime, timedelta 
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_data():
    df = pd.read_csv('/opt/airflow/dags/IOT-temp.csv')
    print("Columns:", df.columns)
    return df

def transform_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='download_data')

    daily_temp = df.groupby('noted_date')['temp'].mean().reset_index()
    hottest_days = daily_temp.nlargest(5, 'temp')
    coldest_days = daily_temp.nsmallest(5, 'temp')

    print("\n" + "="*50)
    print("ТОП 5 САМЫХ ЖАРКИХ ДНЕЙ:")
    print("="*50)
    for idx, row in hottest_days.iterrows():
        print(f"{row['noted_date']}: {row['temp']:.2f}°C")
    
    print("\n" + "="*50)
    print("ТОП 5 САМЫХ ХОЛОДНЫХ ДНЕЙ:")
    print("="*50)
    for idx, row in coldest_days.iterrows():
        print(f"{row['noted_date']}: {row['temp']:.2f}°C")

    df['noted_date'] = pd.to_datetime(df['noted_date'], errors='coerce')

    lower_bound = df['temp'].quantile(0.05)
    upper_bound = df['temp'].quantile(0.95)
    print(f"Границы процентилей: 5% = {lower_bound}, 95% = {upper_bound}") 
    original_count = len(df)
    df = df[(df['temp'] >= lower_bound) & (df['temp'] <= upper_bound)]
    print(f"Удалено выбросов: {original_count - len(df)}")

with DAG('temperature_analysis',
    default_args=default_args,
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=['kaggle', 'temperature', 'iot']
) as dag:
    
    download_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
    )

    transform_task = PythonOperator(
        task_id='transforn_data',
        python_callable=transform_data,
    )

    download_task >> transform_task