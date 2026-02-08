from airflow import DAG
from datetime import datetime, timedelta 
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

    daily_temp = df.groupby('noted_date').agg({
        'temp': ['mean', 'min', 'max', 'count']
    }).reset_index()
    
    daily_temp.columns = ['noted_date', 'avg_temp', 'min_temp', 'max_temp', 'reading_count']
    print(f"Возвращаемый результат тип: {type(daily_temp)}, размер: {len(daily_temp)}")
    return daily_temp

def create_tables():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS daily_temperatures_full (
        id SERIAL PRIMARY KEY,
        noted_date DATE NOT NULL UNIQUE,
        avg_temp DECIMAL(5,2),
        min_temp DECIMAL(5,2),
        max_temp DECIMAL(5,2),
        reading_count INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS daily_temperatures_incremental (
        id SERIAL PRIMARY KEY,
        noted_date DATE NOT NULL UNIQUE,
        avg_temp DECIMAL(5,2),
        min_temp DECIMAL(5,2),
        max_temp DECIMAL(5,2),
        reading_count INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS temperature_extremes (
        id SERIAL PRIMARY KEY,
        noted_date DATE NOT NULL,
        avg_temp DECIMAL(5,2),
        extreme_type VARCHAR(10) CHECK (extreme_type IN ('hottest', 'coldest')),
        rank_position INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    hook = PostgresHook(postgres_conn_id='demo')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    for statement in create_table_sql.split(';'):
        if statement.strip():
            cursor.execute(statement)
    
    conn.commit()
    cursor.close()
    conn.close()

def load_to_postgres_full_hook(**kwargs):
    ti = kwargs['ti']
    daily_temp = ti.xcom_pull(task_ids='transform_data')

    hook = PostgresHook(postgres_conn_id='demo')
    hook.run("TRUNCATE TABLE daily_temperatures_full")
    
    for _, row in daily_temp.iterrows():
        sql = """
        INSERT INTO daily_temperatures_full 
        (noted_date, avg_temp, min_temp, max_temp, reading_count)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (noted_date) DO UPDATE SET
            avg_temp = EXCLUDED.avg_temp,
            min_temp = EXCLUDED.min_temp,
            max_temp = EXCLUDED.max_temp,
            reading_count = EXCLUDED.reading_count,
            updated_at = CURRENT_TIMESTAMP
        """
        hook.run(sql, parameters=(
            row['noted_date'], row['avg_temp'], row['min_temp'], 
            row['max_temp'], row['reading_count']
        ))
    
    print(f"Полная загрузка: загружено {len(daily_temp)} записей")

def load_to_postgres_incremental_hook(**kwargs):
    ti = kwargs['ti']
    daily_temp = ti.xcom_pull(task_ids='transform_data')
    
    cutoff_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
    incremental_df = daily_temp[pd.to_datetime(daily_temp['noted_date']) >= cutoff_date]
    
    if len(incremental_df) == 0:
        print("Нет новых данных для инкрементальной загрузки")
        return
    
    hook = PostgresHook(postgres_conn_id='demo')
    
    for _, row in incremental_df.iterrows():
        sql = """
        INSERT INTO daily_temperatures_incremental 
        (noted_date, avg_temp, min_temp, max_temp, reading_count)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (noted_date) DO UPDATE SET
            avg_temp = EXCLUDED.avg_temp,
            min_temp = EXCLUDED.min_temp,
            max_temp = EXCLUDED.max_temp,
            reading_count = EXCLUDED.reading_count,
            updated_at = CURRENT_TIMESTAMP
        """
        hook.run(sql, parameters=(
            row['noted_date'], row['avg_temp'], row['min_temp'], 
            row['max_temp'], row['reading_count']
        ))
    
    print(f"Инкрементальная загрузка: загружено {len(incremental_df)} записей")

with DAG('temperature_load',
    default_args=default_args,
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=['postgres', 'temperature', 'iot']
) as dag:
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,
    )

    download_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_full_hook = PythonOperator(
        task_id='load_full_hook',
        python_callable=load_to_postgres_full_hook,
    )

    load_incremental_hook = PythonOperator(
        task_id='load_incremental_hook',
        python_callable=load_to_postgres_incremental_hook,
    )

    create_tables_task >> transform_task
    transform_task >> load_full_hook
    transform_task >> load_incremental_hook