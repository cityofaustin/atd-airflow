from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import task_fail_slack_alert

default_args = {
    "owner": "airflow",
    "description": "Fetch data from banner and hr emails and update knack HR app",  # noqa:E501
    "depend_on_past": False,
    "start_date": datetime(2021, 9, 15),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-knack-banner"

# command args
app_name = "hr"
env = "prod"

# assemble env vars
env_vars = Variable.get("atd_knack_banner", deserialize_json=True)
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)
atd_shared_drive = Variable.get("atd_shared_drive", deserialize_json=True)
env_vars["KNACK_APP_NAME"] = app_name
env_vars["KNACK_APP_ID"] = atd_knack_auth[app_name][env]["app_id"]
env_vars["KNACK_API_KEY"] = atd_knack_auth[app_name][env]["api_key"]
env_vars["SHAREDDRIVE_USERNAME"] = atd_shared_drive["SHAREDDRIVE_USERNAME"]
env_vars["SHAREDDRIVE_PASSWORD"] = atd_shared_drive["SHAREDDRIVE_PASSWORD"]
env_vars["SHAREDDRIVE_FILEPATH"] = atd_shared_drive["SHAREDDRIVE_FILEPATH"]


with DAG(
    dag_id="atd_knack_banner",
    default_args=default_args,
    schedule_interval="0 5 * * 0", # is a weekly on sunday update enough?
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "knack", "banner"],
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="atd_knack_banner_update_employees",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-banner/update_employees.py',  # noqa:E501
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1

if __name__ == "__main__":
    dag.cli()
