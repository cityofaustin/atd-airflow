from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import task_fail_slack_alert

default_args = {
    "owner": "airflow",
    "description": "Fetch signal flash statuses KITS and publish to Socata",
    "depend_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-kits:production"

# assemble env vars
env_vars = Variable.get("atd_kits_db", deserialize_json=True)
env_vars["SOCRATA_API_KEY_ID"] = Variable.get("atd_service_bot_socrata_api_key_id")
env_vars["SOCRATA_API_KEY_SECRET"] = Variable.get(
    "atd_service_bot_socrata_api_key_secret"
)
env_vars["SOCRATA_APP_TOKEN"] = Variable.get("atd_service_bot_socrata_app_token")

with DAG(
    dag_id="atd_kits_sig_stat_pub",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "socrata", "kits"],
    catchup=False,
) as dag:
    t1 = DockerOperator(
        task_id="atd_kits_sig_status_to_socrata",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="./atd-kits/atd-kits/signal_status_publisher.py",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1

if __name__ == "__main__":
    dag.cli()
