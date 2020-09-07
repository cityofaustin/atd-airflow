from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator

default_args = {
    "owner": "airflow",
    "description": "Load locations (object_11) records from Knack to S3",
    "depend_on_past": False,
    "start_date": datetime(2018, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

docker_image = "atddocker/atd-knack-services:production"

# command args
script = "records_to_s3"
app_name = "data-tracker"
container = "object_11"
env = "prod"

# assemble env vars
env_vars_socrata = Variable.get("atd_knack_socrata", deserialize_json=True)
env_vars_aws = Variable.get("atd_knack_aws", deserialize_json=True)
env_vars = {**env_vars_socrata, **env_vars_aws}
# unpack knack auth
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)
env_vars["app_id"] = atd_knack_auth[app_name][env]["app_id"]
env_vars["api_key"] = atd_knack_auth[app_name][env]["api_key"]

with DAG(
    dag_id="atd_knack_locations_to_s3",
    default_args=default_args,
    schedule_interval="05 01 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production"],
) as dag:

    date = "{{ prev_execution_date_success or '1970-01-01' }}"

    t1 = DockerOperator(
        task_id="atd_knack_locations_to_s3",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./atd-knack-services/services/{script}.py -a {app_name} -c {container}  -e {env} -d {date}",  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
    )

    t1

if __name__ == "__main__":
    dag.cli()
