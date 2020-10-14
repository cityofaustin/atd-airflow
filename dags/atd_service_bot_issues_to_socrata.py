from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator

default_args = {
    "owner": "airflow",
    "description": "Publish atd-data-tech Github issues to Scorata",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

docker_image = "atddocker/atd-service-bot:production"

# assemble env vars
env_vars = {}
env_vars["GITHUB_ACCESS_TOKEN"] = Variable.get("atd_service_bot_github_token")
env_vars["ZENHUB_ACCESS_TOKEN"] = Variable.get("zenhub_access_token")
env_vars["SOCRATA_API_KEY_ID"] = Variable.get("atd_service_bot_socrata_api_key_id")
env_vars["SOCRATA_API_KEY_SECRET"] = Variable.get(
    "atd_service_bot_socrata_api_key_secret"
)
env_vars["SOCRATA_APP_TOKEN"] = Variable.get("atd_service_bot_socrata_app_token")

with DAG(
    dag_id="atd_service_bot_github_to_socrata_production",
    default_args=default_args,
    schedule_interval="7 7 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "socrata", "atd-service-bot", "github"],
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="dts_github_to_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="./atd-service-bot/issues_to_socrata.py",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1

if __name__ == "__main__":
    dag.cli()
