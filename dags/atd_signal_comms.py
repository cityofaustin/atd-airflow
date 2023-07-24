import os

from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.decorators import task
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = "prod" if os.getenv("ENVIRONMENT") == "production" else "dev"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}


REQUIRED_SECRETS = {
    "AWS_ACCESS_KEY_ID": {
        "opitem": "atd-signal-comms AWS",
        "opfield": f"production.aws_access_key_id",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "atd-signal-comms AWS",
        "opfield": f"production.aws_secret_access_key",
    },
    "BUCKET": {
        "opitem": "atd-signal-comms AWS",
        "opfield": f"production.bucket",
    },
    "KNACK_APP_ID": {
        "opitem": "Knack AMD Data Tracker",
        "opfield": f"production.appId",
    },
    "PGREST_ENDPOINT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.endpoint",
    },
    "PGREST_JWT": {
        "opitem": "atd-knack-services PostgREST",
        "opfield": "production.jwt",
    },
    "SOCRATA_USER": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SOCRATA_PW": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SOCRATA_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
}

@task(
    task_id="get_start_date",
)
def get_start_date(**context):
    """Get the --start date argument. Returns either the prev start date or today
    if the DAG has no successful run history"""
    from pendulum import now
    prev_start_date = context.get("prev_start_date_success") or now()
    return prev_start_date.strftime('%Y-%m-%d')



with DAG(
    dag_id=f"atd_signal_comms",
    description="Ping network devices and publish to S3, then socrata",
    default_args=DEFAULT_ARGS,
    schedule_interval="7 2 * * *" if DEPLOYMENT_ENVIRONMENT == "prod" else None,
    dagrun_timeout=duration(minutes=30),
    tags=["repo:atd-signal-comms", "socrata"],
    catchup=False,
) as dag:
    docker_image = "atddocker/atd-signal-comms:production"

    start_date = get_start_date()
    
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    cameras_s3 = DockerOperator(
        task_id="run_comm_check_cameras",
        image=docker_image,
        auto_remove=True,
        command=f"python atd-signal-comms/run_comm_check.py camera --env {DEPLOYMENT_ENVIRONMENT}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    detectors_s3 = DockerOperator(
        task_id="run_comm_check_detectors",
        image=docker_image,
        auto_remove=True,
        command=f"python atd-signal-comms/run_comm_check.py detector --env {DEPLOYMENT_ENVIRONMENT}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    dms_s3 = DockerOperator(
        task_id="run_comm_check_dms",
        image=docker_image,
        auto_remove=True,
        command=f"python atd-signal-comms/run_comm_check.py digital_message_sign --env {DEPLOYMENT_ENVIRONMENT}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    battery_backup_s3 = DockerOperator(
        task_id="run_comm_check_battery_backup",
        image=docker_image,
        auto_remove=True,
        command=f"python atd-signal-comms/run_comm_check.py cabinet_battery_backup --env {DEPLOYMENT_ENVIRONMENT}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    cameras_socrata = DockerOperator(
        task_id="socata_pub_cameras",
        image=docker_image,
        auto_remove=True,
        command=f"python atd-signal-comms/socrata_pub.py camera --start {start_date} -v --env {DEPLOYMENT_ENVIRONMENT}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    detectors_socrata = DockerOperator(
        task_id="socrata_pub_detectors",
        image=docker_image,
        auto_remove=True,
        command=f"python atd-signal-comms/socrata_pub.py detector --start {start_date} -v --env {DEPLOYMENT_ENVIRONMENT}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    dms_socrata = DockerOperator(
        task_id="socrata_pub_dms",
        image=docker_image,
        auto_remove=True,
        command=f"python atd-signal-comms/socrata_pub.py digital_message_sign --start {start_date} -v --env {DEPLOYMENT_ENVIRONMENT}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    battery_backup_socrata = DockerOperator(
        task_id="socrata_pub_battery_backup",
        image=docker_image,
        auto_remove=True,
        command=f"python atd-signal-comms/socrata_pub.py cabinet_battery_backup --start {start_date} -v --env {DEPLOYMENT_ENVIRONMENT}",
        environment=env_vars,
        tty=True,
        force_pull=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    (
        start_date
        >> cameras_s3
        >> detectors_s3
        >> dms_s3
        >> battery_backup_s3
        >> cameras_socrata
        >> detectors_socrata
        >> dms_socrata
        >> battery_backup_socrata
    )
