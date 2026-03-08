from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from bson import json_util
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1),
}

def replicate_collection(**context):
    """Функция репликации коллекции из MongoDB в PostgreSQL"""
    collection_name = context['params']['collection']
    table_name = context['params']['table']

    mongo_hook = MongoHook(conn_id='mongo_default')
    mongo_client = mongo_hook.get_conn()
    db = mongo_client['ecommerce_db']
    collection = db[collection_name]
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytics')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    
    cursor.execute("""
        SELECT last_loaded_at FROM replication_control 
        WHERE collection_name = %s
    """, (collection_name,))
    result = cursor.fetchone()
    
    last_loaded = result[0] if result else datetime(2000, 1, 1)
    logging.info(f"Последняя загрузка для {collection_name}: {last_loaded}")
    documents = list(collection.find())
    
    if not documents:
        logging.info(f"Нет новых документов для {collection_name}")
        return
    
    logging.info(f"Найдено {len(documents)} документов для {collection_name}")
    
    for doc in documents:
        doc['_id'] = str(doc['_id'])
        
        if collection_name == 'user_sessions':
            replicate_user_sessions(cursor, doc)
        elif collection_name == 'support_tickets':
            replicate_support_tickets(cursor, doc)
        elif collection_name == 'event_logs':
            replicate_event_logs(cursor, doc)
        elif collection_name == 'user_recommendations':
            replicate_user_recommendations(cursor, doc)
        elif collection_name == 'moderation_queue':
            replicate_moderation_queue(cursor, doc)

    max_date = max([doc.get('created_at', datetime.now()) for doc in documents])
    cursor.execute("""
        INSERT INTO replication_control (collection_name, last_loaded_at, records_loaded)
        VALUES (%s, %s, %s)
        ON CONFLICT (collection_name) DO UPDATE SET
            last_loaded_at = EXCLUDED.last_loaded_at,
            records_loaded = replication_control.records_loaded + EXCLUDED.records_loaded
    """, (collection_name, max_date, len(documents)))
    
    pg_conn.commit()
    cursor.close()
    pg_conn.close()
    logging.info(f"✅ Загружено {len(documents)} записей в {table_name}")

def replicate_user_sessions(cursor, doc) -> None:
    cursor.execute("""
            INSERT INTO user_sessions (session_id, user_id, start_time, end_time, 
                                        pages_visited, device, actions, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                end_time = EXCLUDED.end_time,
                pages_visited = EXCLUDED.pages_visited,
                actions = EXCLUDED.actions
        """, (
            doc['session_id'],
            doc['user_id'],
            doc['start_time'],
            doc['end_time'],
            json.dumps(doc.get('pages_visited', [])),
            json.dumps(doc.get('device', {})),
            json.dumps(doc.get('actions', [])),
            doc.get('created_at', datetime.now())
        ))
        
def replicate_support_tickets(cursor, doc) -> None:
    cursor.execute("""
            INSERT INTO support_tickets (ticket_id, user_id, status, issue_type, 
                                        messages, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO UPDATE SET
                status = EXCLUDED.status,
                messages = EXCLUDED.messages,
                updated_at = EXCLUDED.updated_at
        """, (
            doc['ticket_id'],
            doc['user_id'],
            doc['status'],
            doc['issue_type'],
            json.dumps(doc.get('messages', [])),
            doc['created_at'],
            doc['updated_at']
        ))

def replicate_event_logs(cursor, doc) -> None:
    cursor.execute("""
                INSERT INTO event_logs (
                    event_id, timestamp, event_type, user_id, 
                    session_id, details, loaded_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
            """, (
                doc.get('event_id'),
                doc.get('created_at'),
                doc.get('event_type'),
                doc.get('user_id'),
                doc.get('session_id'),
                json.dumps(doc.get('details', {})),
                datetime.now()
            ))
    
def replicate_user_recommendations(cursor, doc) -> None:
    cursor.execute("""
                INSERT INTO user_recommendations (
                    user_id, recommended_products, last_updated, loaded_at
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                    recommended_products = EXCLUDED.recommended_products,
                    last_updated = EXCLUDED.last_updated,
                    loaded_at = EXCLUDED.loaded_at
            """, (
                doc.get('user_id'),
                json.dumps(doc.get('recommended_products', [])),
                doc.get('last_updated'),
                datetime.now()
            ))

def replicate_moderation_queue(cursor, doc) -> None:
    cursor.execute("""
                INSERT INTO moderation_queue (
                    review_id, user_id, product_id, review_text,
                    rating, moderation_status, flags, submitted_at, loaded_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (review_id) DO UPDATE SET
                    moderation_status = EXCLUDED.moderation_status,
                    flags = EXCLUDED.flags,
                    loaded_at = EXCLUDED.loaded_at
            """, (
                doc.get('review_id'),
                doc.get('user_id'),
                doc.get('product_id'),
                doc.get('review_text', ''),
                doc.get('rating', 0),
                doc.get('moderation_status', 'pending'),
                json.dumps(doc.get('flags', [])),
                doc.get('submitted_at'),
                datetime.now()
            ))
        
with DAG(
    'data_replication_dag',
    default_args=default_args,
    description='Репликация данных из MongoDB в PostgreSQL',
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=['replication', 'mongodb', 'postgres'],
) as dag:
    replicate_sessions = PythonOperator(
        task_id='replicate_user_sessions',
        python_callable=replicate_collection,
        params={'collection': 'user_sessions', 'table': 'user_sessions'}
    )
    
    replicate_events = PythonOperator(
        task_id='replicate_event_logs',
        python_callable=replicate_collection,
        params={'collection': 'event_logs', 'table': 'event_logs'}
    )
    
    replicate_tickets = PythonOperator(
        task_id='replicate_support_tickets',
        python_callable=replicate_collection,
        params={'collection': 'support_tickets', 'table': 'support_tickets'}
    )
    
    replicate_recommendations = PythonOperator(
        task_id='replicate_user_recommendations',
        python_callable=replicate_collection,
        params={'collection': 'user_recommendations', 'table': 'user_recommendations'}
    )
    
    replicate_moderation = PythonOperator(
        task_id='replicate_moderation_queue',
        python_callable=replicate_collection,
        params={'collection': 'moderation_queue', 'table': 'moderation_queue'}
    )
    
    replicate_sessions >> replicate_events >> replicate_tickets >> replicate_recommendations >> replicate_moderation
