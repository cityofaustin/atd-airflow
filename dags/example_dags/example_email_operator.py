from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='example_email_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['example']
)


email_task = EmailOperator(
    to='your@email.com',
    task_id='email_task',
    subject='Templated Subject: start_date {{ ds }}',
    mime_charset="utf-8",
    params={'content1': 'random'},
    html_content="Templated Content: content1 - {{ params.content1 }}  task_key - {{ task_instance_key_str }} test_mode - {{ test_mode }} task_owner - {{ task.owner}} hostname - {{ ti.hostname }}",
    dag=dag
)