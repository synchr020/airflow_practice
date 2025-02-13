from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import requests

def slack_on_success(context):
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    msg = f":white_check_mark: *DAG* **{dag_id}** đã chạy thành công vào *{execution_date}*."
    slack_alert = SlackWebhookOperator(
        task_id='slack_notify_success',
        webhook_token="slack_connection", 
        message=msg,
        username='airflow'
    )
    slack_alert.execute(context=context)

def slack_on_failure(context):
    dag_id = context.get('dag').dag_id
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url
    msg = (f":x: *Task* **{task_id}** trong DAG *{dag_id}* thất bại vào *{execution_date}*.\n"
           f"Log: {log_url}")
    slack_alert = SlackWebhookOperator(
        task_id='slack_notify_failure',
        webhook_token="slack_connection",  
        message=msg,
        username='airflow'
    )
    slack_alert.execute(context=context)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 12),
}

dag = DAG(
    'etl_pipeline_import',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),
    catchup=False,
    on_success_callback=slack_on_success,
    on_failure_callback=slack_on_failure,
)

def extract_csv1():
    file_path = '/opt/airflow/data/data1.csv'
    if os.path.exists(file_path):
        try:
            df1 = pd.read_csv(file_path)
            print("data1.csv đã được nhập thành công.")
        except Exception as e:
            print("Lỗi khi nhập data1.csv:", e)
            raise
    else:
        raise FileNotFoundError(f"{file_path} không tồn tại.")

def extract_csv2():
    api_url = "https://retoolapi.dev/ghIzE8/data2"
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        
        data = response.json()
        df2 = pd.DataFrame(data)
        
        df2.to_csv('/opt/airflow/data/data2.csv', index=False)
        print("data2.csv đã được tạo từ dữ liệu API (JSON).")
        
    except Exception as e:
        print("Lỗi khi lấy dữ liệu từ API:", e)
        raise

def transform_data():
    try:
        df1 = pd.read_csv('/opt/airflow/data/data1.csv')
        df2 = pd.read_csv('/opt/airflow/data/data2.csv')
    except Exception as e:
        print("Lỗi khi đọc file CSV trong transform_data:", e)
        raise
    
    df1['name'] = df1['name'].str.lower()
    
    df1.to_csv('/opt/airflow/data/data1_transformed.csv', index=False)
    df2.to_csv('/opt/airflow/data/data2_transformed.csv', index=False)
    print("Dữ liệu đã được chuẩn hoá và lưu thành data1_transformed.csv, data2_transformed.csv.")

def combine_data():
    try:
        df1 = pd.read_csv('/opt/airflow/data/data1_transformed.csv')
        df2 = pd.read_csv('/opt/airflow/data/data2_transformed.csv')
    except Exception as e:
        print("Lỗi khi đọc file CSV đã chuẩn hoá:", e)
        raise
    
    # Kết hợp dữ liệu theo cột 'id'
    combined_df = pd.merge(df1, df2, on='id')
    combined_df.to_csv('/opt/airflow/data/combined.csv', index=False)
    print("Dữ liệu đã được kết hợp và lưu thành combined.csv.")

def load_to_postgres():
    try:
        df = pd.read_csv('/opt/airflow/data/combined.csv')
    except Exception as e:
        print("Lỗi khi đọc file combined.csv:", e)
        raise
    
    run_id = str(uuid.uuid4())
    
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='airflow_db',
        user='airflow',
        password='airflow'
    )
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS combined (
            id INT,
            name TEXT,
            age INT,
            run_id TEXT,
            PRIMARY KEY (id, run_id)
        );
    """)
    
    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO combined (id, name, age, run_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id, run_id) DO NOTHING;
            """,
            (int(row['id']), row['name'], int(row['age']), run_id)
        )
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Dữ liệu của phiên chạy {run_id} đã được lưu vào Postgres.")

extract_task1 = PythonOperator(
    task_id='import_csv1',
    python_callable=extract_csv1,
    dag=dag,
)

extract_task2 = PythonOperator(
    task_id='import_csv2',
    python_callable=extract_csv2,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

combine_task = PythonOperator(
    task_id='combine_data',
    python_callable=combine_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

[extract_task1, extract_task2] >> transform_task >> combine_task >> load_task
