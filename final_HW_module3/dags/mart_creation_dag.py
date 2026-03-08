from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'data_analytics',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

def check_data_quality(**context):
    """Проверка качества данных перед построением витрин"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytics')
    today = datetime.now().date()
    
    sessions_count = pg_hook.get_first("""
        SELECT COUNT(*) FROM user_sessions 
        WHERE DATE(start_time) = %s
    """, parameters=(today,))[0]
    
    if sessions_count == 0:
        raise ValueError(f"Нет данных за {today} в user_sessions!")
    
    logging.info(f"✅ Data quality check passed: {sessions_count} sessions")
    return True

def build_user_activity_mart(**context):
    """Построение витрины активности пользователей"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytics')
    
    sql = """
    DROP TABLE IF EXISTS marts.user_activity_mart CASCADE;
    
    CREATE TABLE marts.user_activity_mart AS
    WITH daily_activity AS (
        SELECT 
            us.user_id,
            DATE(us.start_time) as activity_date,
            COUNT(DISTINCT us.session_id) as sessions_count,
            AVG(EXTRACT(EPOCH FROM (us.end_time - us.start_time))/60) as avg_session_duration,
            COUNT(el.event_id) as events_count
        FROM user_sessions us
        LEFT JOIN event_logs el ON us.session_id = el.session_id
        WHERE us.start_time >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY 1, 2
    )
    SELECT * FROM daily_activity;
    """
    
    pg_hook.run(sql)
    logging.info("✅ User activity mart built successfully")

def build_support_mart(**context):
    """Построение витрины поддержки"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytics')
    
    sql = """
    DROP TABLE IF EXISTS marts.support_efficiency_mart CASCADE;
    
    CREATE TABLE marts.support_efficiency_mart AS
    SELECT 
        DATE(created_at) as ticket_date,
        status,
        issue_type,
        COUNT(*) as tickets_count,
        AVG(EXTRACT(EPOCH FROM (updated_at - created_at))/3600) as avg_resolution_hours
    FROM support_tickets
    WHERE created_at >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY 1, 2, 3;
    """
    
    pg_hook.run(sql)
    logging.info("✅ Support efficiency mart built successfully")

with DAG(
    'mart_creation_dag',
    default_args=default_args,
    description='Построение аналитических витрин',
    schedule=timedelta(hours=3),
    catchup=False,
    tags=['marts', 'analytics'],
) as dag:
    
    check_quality = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
    )
    
    build_user_mart = PythonOperator(
        task_id='build_user_activity_mart',
        python_callable=build_user_activity_mart,
    )
    
    build_support_mart = PythonOperator(
        task_id='build_support_efficiency_mart',
        python_callable=build_support_mart,
    )
    
    check_quality >> [build_user_mart, build_support_mart]