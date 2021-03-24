from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import task_fail_slack_alert

DEFAULT_ARGS = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2020, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

DOCKER_IMAGE = "atddocker/atd-knack-services:production"
SCRIPT_TASK_1 = "records_to_postgrest"
SCRIPT_TASK_2 = "records_to_agol"
APP_NAME = "signs-markings"
ENV = "prod"
POOL_KNACK = "knack_signs_markings"
POOL_POSTGREST = "atd_knack_postgrest_pool"
CONTAINER = "view_3096"

env_vars = Variable.get("atd_knack_services_postgrest", deserialize_json=True)
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)
env_vars["KNACK_APP_ID"] = atd_knack_auth[APP_NAME][ENV]["app_id"]
env_vars["KNACK_API_KEY"] = atd_knack_auth[APP_NAME][ENV]["api_key"]
env_vars["AGOL_USERNAME"] = Variable.get("agol_username")
env_vars["AGOL_PASSWORD"] = Variable.get("agol_password")

with DAG(
    dag_id="atd_knack_markings_attachments",
    description="Loads markings attachments records from Knack to Postgrest to AGOL",  # noqa:E501
    default_args=DEFAULT_ARGS,
    schedule_interval="30 4 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "knack", "agol"],
    catchup=False,
) as dag:
    # completely replace data on 15th day of every month
    # this is a failsafe catch records that may have been missed via incremental
    # loading
    date_filter = "{{ '1970-01-01' if ds.endswith('15') else prev_execution_date_success or '1970-01-01' }}"  # noqa:E501

    t1 = DockerOperator(
        task_id=f"markings_attachments_{SCRIPT_TASK_1}",
        image=DOCKER_IMAGE,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{SCRIPT_TASK_1}.py -a {APP_NAME} -c {CONTAINER} -d "{date_filter}"',  # noqa:E501
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        pool=POOL_KNACK,
        tty=True,
        dag=dag,
    )

    t2 = DockerOperator(
        task_id=f"markings_attachments_{SCRIPT_TASK_2}",
        image=DOCKER_IMAGE,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{SCRIPT_TASK_2}.py -a {APP_NAME} -c {CONTAINER} -d "{date_filter}"',  # noqa:E501
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        pool=POOL_POSTGREST,
        tty=True,
        dag=dag,
    )

    t1 >> t2


if __name__ == "__main__":
    dag.cli()
