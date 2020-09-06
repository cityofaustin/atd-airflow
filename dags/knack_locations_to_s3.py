from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "description": "Load locations (object_11) records from Knack to S3",
    "depend_on_past": False,
    "start_date": datetime(2018, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

docker_image = "atddocker/atd-knack-services"

# gather env vars
env_vars = {}
env_vars["KNACK_CREDIENTIALS"] = Variable.get("KNACK_CREDENTIALS", deserialize_json=False)
env_vars["SOCRATA_USERNAME"] = Variable.get("SOCRATA_USERNAME")
env_vars["SOCRATA_PASSWORD"] = Variable.get("SOCRATA_PASSWORD")
env_vars["aws_access_key_id"] = Variable.get("aws_access_key_id")
env_vars["aws_secret_access_key"] = Variable.get("aws_secret_access_key")

# define command args
script = "records_to_s3"
app_name = "data-tracker"
container = "object_11"
env = "prod"

with DAG(
    dag_id="locations_to_s3",
    default_args=default_args,
    schedule_interval="05 01 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production"],
) as dag:

    date = "{{ prev_execution_date_success }}"

    t1 = DockerOperator(
        task_id="locations_to_s3",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./services/services/{script}.py -a {app_name} -c {container}  -e {env} -d {date}",
        network_mode="bridge",
        environment=env_vars,
    )

    t1

if __name__ == "__main__":
    dag.cli()
