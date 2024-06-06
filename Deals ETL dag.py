from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
 
try:
    
    from store_into_s3bucket import main as store_into_s3bucket
    from extract_from_s3_and_transform import main as extract_from_s3_and_transform
    from insert_docs_to_OpenSearch import connect_to_OpenSearch_and_add_deals as insert_docs_to_OpenSearch
    from Extract_from_API import get_all_tasks_and_store_to_files 
except ImportError as e:
    print(f"Error importing modules: {e}")

default_args = {
    'owner': 'Tomer Dagan',
    'retries': 0, 
    'retry_delay': timedelta(minutes=1)
}
 
def wrapper_extract_from_s3_and_transform(**kwargs):

    extract_from_s3_and_transform()#all_deals = extract_from_s3_and_transform()

def wrapper_insert_docs_to_OpenSearch(**kwargs):
    insert_docs_to_OpenSearch()#deals_to_load_to_OpenSearch


with DAG(
    dag_id='Deals_ETL',
    default_args=default_args,
    description='This is DAG of DEALS ETL....',
    start_date=datetime(2024, 1, 17, 12),
    schedule_interval='@weekly',
    catchup=False  # Prevents running missed DAG runs on startup
) as dag:
    
    Extract_from_API_task = PythonOperator(
        task_id='Extract_from_API_task',
        python_callable=get_all_tasks_and_store_to_files
    )

    store_to_s3bucket_task = PythonOperator(
        task_id='store_to_s3bucket_task',
        python_callable=store_into_s3bucket
    )
    
    extract_from_s3_and_transform_task = PythonOperator(
        task_id='extract_from_s3_and_transform_task',
        python_callable=wrapper_extract_from_s3_and_transform,
        provide_context=True
    )

    insert_deals_to_OpenSearch_task = PythonOperator(
        task_id='insert_deals_to_OpenSearch_task',
        python_callable=wrapper_insert_docs_to_OpenSearch,
        provide_context=True
    )
    
    Extract_from_API_task >> store_to_s3bucket_task >> extract_from_s3_and_transform_task >> insert_deals_to_OpenSearch_task
