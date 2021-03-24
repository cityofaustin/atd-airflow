from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import task_fail_slack_alert

default_args = {
    "owner": "airflow",
    "description": "Load mmc activities from Knack to Poostgres to Socrata",
    "depend_on_past": False,
    "start_date": datetime(2020, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-knack-services:production"

# command args
task_1_script = "records_to_postgrest"
task_2_script = "records_to_socrata"
app_name = "data-tracker"
container = "view_2681"
env = "prod"

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
    dag_id="atd_knack_mmc_activities_to_s3_to_socrata",
    default_args=default_args,
    schedule_interval="20 6 * * *",
    dagrun_timeout=timedelta(minutes=300),
    tags=["production", "knack"],
    catchup=False,
) as dag:

    date = "{{ prev_execution_date_success or '1970-01-01' }}"

    t1 = DockerOperator(
        task_id="atd_knack_mmc_activities_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{task_1_script}.py -a {app_name} -c {container} -d "{date}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t2 = DockerOperator(
        task_id="atd_knack_mmc_activities_to_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{task_2_script}.py -a {app_name} -c {container} -d "{date}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1 >> t2

if __name__ == "__main__":
    dag.cli()
