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

docker_image = "atddocker/atd-knack-banner" # the old one said atd-knack-services:production, what is the : for?

# command args
app_name = "hr"
env = "prod"

# assemble env vars
env_vars = Variable.get("atd_knack_services_postgrest", deserialize_json=True) # check the format of this
env_vars = {} # ? 
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)
env_vars["KNACK_APP_NAME"] = app_name
env_vars["KNACK_APP_ID"] = atd_knack_auth[app_name][env]["app_id"]
env_vars["KNACK_API_KEY"] = atd_knack_auth[app_name][env]["api_key"]

# BANNER_API_KEY=
# BANNER_URL=
# SHAREDDRIVE_USERNAME=
# SHAREDDRIVE_PASSWORD=
# SHAREDDRIVE_FILEPATH=


with DAG(
    dag_id="atd_knack_banner",
    default_args=default_args, #check this
    schedule_interval="0 5 * * 0", #todo: update, weekly on sunday? when
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "knack", "banner"],
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="atd_knack_banner_update_employees",
        image=docker_image,
        api_version="auto", #check this
        auto_remove=True, #and this
        command=f'./atd-knack-banner/update_employees.py',  # noqa:E501
        docker_url="tcp://localhost:2376", #check this
        network_mode="bridge", # check this
        environment=env_vars,
        tty=True, #hm?
    )

    t1

if __name__ == "__main__":
    dag.cli()
