from os import getenv

from utils.docker_operator import DockerOperatorWithFallback
from airflow.sdk import dag, task
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT", "development")

# Define a similar variable with an abbreviated stage name for use in commands
deployment_stage_abbreviation = (
    "prod" if DEPLOYMENT_ENVIRONMENT == "production" else "dev"
)

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 1, 1, tz="America/Chicago"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=20),
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


@task(task_id="get_start_date")
def get_start_date(**context):
    """Return the '--start' date for Socrata publish tasks (previous successful run or today)."""
    from pendulum import now

    prev_start_date = context.get("prev_start_date_success") or now()
    return prev_start_date.strftime("%Y-%m-%d")


@dag(
    dag_id="atd_signal_comms",
    description="Ping network devices and publish to S3, then socrata",
    default_args=DEFAULT_ARGS,
    schedule="7 2 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-signal-comms", "socrata"],
    catchup=False,
    doc_md="""
## Signal communications (S3 and Socrata)

Runs 'atddocker/atd-signal-comms:production' to check device communications,
write results to S3, then publish incremental updates to Socrata.

### Network requirement

This DAG must run from an environment that can reach the Signal network. Without Signal
network connectivity, the 'run_comm_check.py' tasks cannot poll devices and will fail.

### New Airflow environments

The 'get_start_date' task uses 'prev_start_date_success' from the task context so Socrata
publishes incrementally from the last successful run. A new Airflow metadata database has
no prior successful runs, so the first run falls back to 'today' and may not match the
incremental window you expect. When moving this DAG to a new environment, add an artificial
successful run (or otherwise establish the baseline you want) before relying on incremental
Socrata loads.

""",
)
def atd_signal_comms():
    docker_image = "atddocker/atd-signal-comms:production"

    start_date = get_start_date()
    env_vars = get_env_vars_task(REQUIRED_SECRETS)

    cameras_s3 = DockerOperator(
        task_id="run_comm_check_cameras",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python atd-signal-comms/run_comm_check.py camera --env {deployment_stage_abbreviation}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    detectors_s3 = DockerOperator(
        task_id="run_comm_check_detectors",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python atd-signal-comms/run_comm_check.py detector --env {deployment_stage_abbreviation}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    dms_s3 = DockerOperator(
        task_id="run_comm_check_dms",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python atd-signal-comms/run_comm_check.py digital_message_sign --env {deployment_stage_abbreviation}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    battery_backup_s3 = DockerOperator(
        task_id="run_comm_check_battery_backup",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python atd-signal-comms/run_comm_check.py cabinet_battery_backup --env {deployment_stage_abbreviation}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    signal_monitors_s3 = DockerOperator(
        task_id="run_comm_check_signal_monitors",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python atd-signal-comms/run_comm_check.py signal_monitor --env {deployment_stage_abbreviation}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    cameras_socrata = DockerOperator(
        task_id="socrata_pub_cameras",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python atd-signal-comms/socrata_pub.py camera --start {start_date} -v --env {deployment_stage_abbreviation}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    detectors_socrata = DockerOperator(
        task_id="socrata_pub_detectors",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python atd-signal-comms/socrata_pub.py detector --start {start_date} -v --env {deployment_stage_abbreviation}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    dms_socrata = DockerOperator(
        task_id="socrata_pub_dms",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python atd-signal-comms/socrata_pub.py digital_message_sign --start {start_date} -v --env {deployment_stage_abbreviation}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    battery_backup_socrata = DockerOperator(
        task_id="socrata_pub_battery_backup",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python atd-signal-comms/socrata_pub.py cabinet_battery_backup --start {start_date} -v --env {deployment_stage_abbreviation}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    signal_monitors_socrata = DockerOperator(
        task_id="socrata_pub_signal_monitors",
        image=docker_image,
        docker_conn_id="docker_default",
        auto_remove="force",
        command=f"python atd-signal-comms/socrata_pub.py signal_monitor --start {start_date} -v --env {deployment_stage_abbreviation}",
        environment=env_vars,
        tty=True,
        mount_tmp_dir=False,
        network_mode="bridge",
    )

    (
        start_date
        >> cameras_s3
        >> detectors_s3
        >> dms_s3
        >> battery_backup_s3
        >> signal_monitors_s3
        >> cameras_socrata
        >> detectors_socrata
        >> dms_socrata
        >> battery_backup_socrata
        >> signal_monitors_socrata
    )


atd_signal_comms()
