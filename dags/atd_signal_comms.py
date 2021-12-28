from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import *

default_args = {
    "owner": "airflow",
    "description": "Ping network devices and publish to S3, then socrata",
    "depend_on_past": False,
    "start_date": datetime(2021, 12, 13),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-signal-comms:production"
env = "prod"
env_vars = Variable.get("atd_signal_comms", deserialize_json=True)

with DAG(
    dag_id="atd_signal_comms",
    default_args=default_args,
    schedule_interval="7 7 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "amd"],
    catchup=False,
) as dag:
    start_date = "{{ prev_execution_date_success.strftime('%Y-%m-%d') if prev_execution_date_success else '2021-12-13'}}"

    cameras_s3 = DockerOperator(
        task_id="run_comm_check_cameras",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="python atd-signal-comms/run_comm_check.py camera --env prod",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    detectors_s3 = DockerOperator(
        task_id="run_comm_check_detectors",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="python atd-signal-comms/run_comm_check.py detector --env prod",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    dms_s3 = DockerOperator(
        task_id="run_comm_check_dms",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="python atd-signal-comms/run_comm_check.py digital_message_sign --env prod",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    battery_backup_s3 = DockerOperator(
        task_id="run_comm_check_battery_backup",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command="python atd-signal-comms/run_comm_check.py cabinet_battery_backup --env prod",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    cameras_socrata = DockerOperator(
        task_id="socata_pub_cameras",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"python atd-signal-comms/socrata_pub.py camera --start {start_date} --env prod",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    detectors_socrata = DockerOperator(
        task_id="socrata_pub_detectors",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"python atd-signal-comms/socrata_pub.py detector --start {start_date} --env prod",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    dms_socrata = DockerOperator(
        task_id="socrata_pub_dms",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"python atd-signal-comms/socrata_pub.py digital_message_sign --start {start_date} --env prod",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    battery_backup_socrata = DockerOperator(
        task_id="socrata_pub_battery_backup",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"python atd-signal-comms/socrata_pub.py cabinet_battery_backup --start {start_date} --env prod",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    """
    the socrata pub is more likely to fail than ping/s3 upload, so run ping/s3 first
    socrata can be backfilled but ping attempts are point-in-time only
    """
    cameras_s3 >> detectors_s3 >> dms_s3 >> battery_backup_s3 >> cameras_socrata >> detectors_socrata >> dms_socrata >> battery_backup_socrata

if __name__ == "__main__":
    dag.cli()
