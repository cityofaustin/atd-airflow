from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator

default_args = {
    "owner": "airflow",
    "description": "Load signals (view_197) records from Knack to S3",
    "depend_on_past": False,
    "start_date": datetime(2020, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

docker_image = "atddocker/atd-knack-services:production"

# command args
script = "records_to_s3"
app_name = "data-tracker"
container = "view_197"
env = "prod"

# assemble env vars
env_vars = Variable.get("atd_knack_aws", deserialize_json=True)
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)
env_vars["KNACK_APP_ID"] = atd_knack_auth[app_name][env]["app_id"]
env_vars["KNACK_API_KEY"] = atd_knack_auth[app_name][env]["api_key"]

with DAG(
    dag_id="atd_knack_signals_to_s3",
    default_args=default_args,
    schedule_interval="11 01 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "knack"],
    catchup=False,
) as dag:

    date = "{{ prev_execution_date_success or '1970-01-01' }}"

    t1 = DockerOperator(
        task_id="atd_knack_signals_to_s3",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script}.py -a {app_name} -c {container}  -e {env} -d "{date}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1

if __name__ == "__main__":
    dag.cli()
