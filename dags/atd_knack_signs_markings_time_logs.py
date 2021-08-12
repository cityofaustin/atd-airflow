from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import task_fail_slack_alert

default_args = {
    "owner": "airflow",
    "description": "Load signs markings time logs (view_3516) records from Knack to Postgrest to Socrata",  # noqa:E501
    "depend_on_past": False,
    "start_date": datetime(2021, 7, 28),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-knack-services:production"

# command args
script_task_1 = "records_to_postgrest"
script_task_2 = "records_to_socrata"
app_name = "signs-markings"
container_markings = "view_3307"
container_signs = "view_3528"
env = "prod"

# use pooling to throttle DB load
POOL_KNACK = "knack_signs_markings"
POOL_POSTGREST = "atd_knack_postgrest_pool"

# assemble env vars
env_vars = Variable.get("atd_knack_services_postgrest", deserialize_json=True)
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)
env_vars["KNACK_APP_ID"] = atd_knack_auth[app_name][env]["app_id"]
env_vars["KNACK_API_KEY"] = atd_knack_auth[app_name][env]["api_key"]
env_vars["SOCRATA_API_KEY_ID"] = Variable.get("atd_service_bot_socrata_api_key_id")
env_vars["SOCRATA_API_KEY_SECRET"] = Variable.get(
    "atd_service_bot_socrata_api_key_secret"
)
env_vars["SOCRATA_APP_TOKEN"] = Variable.get("atd_service_bot_socrata_app_token")

with DAG(
    dag_id="atd_knack_signs_markings_time_logs",
    default_args=default_args,
    schedule_interval="50 14,18 * * *",  # runs once at 950a cst and again at 150pm cst
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "knack"],
    catchup=False,
) as dag:
    # completely replace data on 15th day of every month
    # this is a failsafe catch records that may have been missed via incremental loading
    date_filter = "{{ '1970-01-01' if ds.endswith('15') else prev_execution_date_success or '1970-01-01' }}"  # noqa:E501
    t1 = DockerOperator(
        task_id="atd_knack_markings_time_logs_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_1}.py -a {app_name} -c {container_markings} -d "{date_filter}"',  # noqa:E501
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        pool=POOL_POSTGREST,
        tty=True,
    )

    t2 = DockerOperator(
        task_id="atd_knack_markings_time_logs_to_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_2}.py -a {app_name} -c {container_markings} -d "{date_filter}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        pool=POOL_KNACK,
        tty=True,
    )

    t3 = DockerOperator(
        task_id="atd_knack_signs_time_logs_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_1}.py -a {app_name} -c {container_signs} -d "{date_filter}"',  # noqa:E501
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        pool=POOL_POSTGREST,
        tty=True,
    )

    t4 = DockerOperator(
        task_id="atd_knack_signs_time_logs_to_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_2}.py -a {app_name} -c {container_signs} -d "{date_filter}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        pool=POOL_KNACK,
        tty=True,
    )

    t1 >> t2 >> t3 >> t4

if __name__ == "__main__":
    dag.cli()
