from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator

default_args = {
    "owner": "airflow",
    "description": "Fetch road condition sensor data from postgrest and publish to socrata ",  # noqa:E501
    "depend_on_past": False,
    "start_date": datetime(2020, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

docker_image = "atddocker/atd-road-conditions:production"


# assemble env vars
env_vars = Variable.get("atd_road_conditions_postgrest", deserialize_json=True)
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)
env_vars["KNACK_APP_ID"] = atd_knack_auth["data-tracker"]["prod"]["app_id"]
env_vars["KNACK_API_KEY"] = atd_knack_auth["data-tracker"]["prod"]["api_key"]
env_vars["SOCRATA_API_KEY_ID"] = Variable.get("atd_service_bot_socrata_api_key_id")
env_vars["SOCRATA_API_KEY_SECRET"] = Variable.get(
    "atd_service_bot_socrata_api_key_secret"
)
env_vars["SOCRATA_APP_TOKEN"] = Variable.get("atd_service_bot_socrata_app_token")

with DAG(
    dag_id="road_conditions_socrata",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "road-conditions"],
    catchup=False,
) as dag:

    date_filter = "{{ prev_execution_date_success or '2020-12-31' }}"

    t1 = DockerOperator(
        task_id="road_conditions_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-road-conditions/socrata.py -d "{date_filter}"',
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1

if __name__ == "__main__":
    dag.cli()
