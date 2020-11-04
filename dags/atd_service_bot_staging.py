from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator

default_args = {
    "owner": "airflow",
    "description": "Fetch new DTS service requests and create Github issues",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

docker_image = "atddocker/atd-service-bot:production"
app_name = "dts-portal"
env = "test"

# assemble env vars
env_vars = {}
KNACK_AUTH = Variable.get("atd_knack_auth", deserialize_json=True)
env_vars["KNACK_APP_ID"] = KNACK_AUTH[app_name][env]["app_id"]
env_vars["KNACK_API_KEY"] = KNACK_AUTH[app_name][env]["api_key"]
env_vars["GITHUB_ACCESS_TOKEN"] = Variable.get("atd_service_bot_github_token")
env_vars["KNACK_DTS_PORTAL_SERVICE_BOT_USERNAME"] = Variable.get(
    "knack_dts_portal_service_bot_username"
)
env_vars["KNACK_DTS_PORTAL_SERVICE_BOT_PASSWORD"] = Variable.get(
    "knack_dts_portal_service_bot_password"
)

with DAG(
    dag_id="atd_service_bot_issue_intake_production",
    default_args=default_args,
    schedule_interval="* * * * *",
    dagrun_timeout=timedelta(minutes=5),
    tags=["staging", "knack", "atd-service-bot", "github"],
    catchup=False,
) as dag:

    t1 = DockerOperator(
        task_id="dts_sr_to_github",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="./atd-service-bot/intake.py",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1

if __name__ == "__main__":
    dag.cli()
