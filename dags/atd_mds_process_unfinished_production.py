from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

from _slack_operators import *

default_args = {
    "owner": "airflow",
    "description": "Process unfinished tasks",
    "depend_on_past": False,
    "start_date": datetime(2018, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

current_time = datetime.now()
current_time_min = current_time + timedelta(days=-35)
current_time_max = current_time + timedelta(days=-1, hours=-8)
time_min = f"{current_time_min.year}-{current_time_min.month}-{current_time_min.day}-01"
time_max = f"{current_time_max.year}-{current_time_max.month}-{current_time_max.day}-{current_time_max.hour}"

# Calculate the dates we need to gather data for
socrata_sync_date_start = current_time + timedelta(days=-35)
socrata_sync_date_end = current_time + timedelta(days=1)
socrata_sync_date_format = "%Y-%m-%d"

environment_vars = Variable.get("atd_mds_config_production", deserialize_json=True)
docker_image = "atddocker/atd-mds-etl:production"

with DAG(
    f"atd_mds_process_unfinished_production",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["production", "mds"],
) as dag:
    # Task: process_unfinished_lime
    # Description: Processes unfinished schedule blocks assigned to Lime
    lime = DockerOperator(
        task_id="process_unfinished_lime",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./provider_runtool.py --provider 'lime' --time-min '{time_min}' --time-max '{time_max}' --incomplete-only --no-logs",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=environment_vars,
    )

    # Task: process_unfinished_jump
    # Description: Processes unfinished schedule blocks assigned to Jump
    jump = DockerOperator(
        task_id="process_unfinished_jump",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./provider_runtool.py --provider 'jump' --time-min '{time_min}' --time-max '{time_max}' --incomplete-only --no-logs",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=environment_vars,
    )

    # Task: process_unfinished_bird
    # Description: Processes unfinished schedule blocks assigned to Bird
    bird = DockerOperator(
        task_id="process_unfinished_bird",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./provider_runtool.py --provider 'bird' --time-min '{time_min}' --time-max '{time_max}' --incomplete-only --no-logs",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=environment_vars,
    )

    # Task: process_unfinished_lyft
    # Description: Processes unfinished schedule blocks assigned to Lyft
    lyft = DockerOperator(
        task_id="process_unfinished_lyft",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./provider_runtool.py --provider 'lyft' --time-min '{time_min}' --time-max '{time_max}' --incomplete-only --no-logs",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=environment_vars,
    )

    # Task: process_unfinished_wheels
    # Description: Processes unfinished schedule blocks assigned to Wheels
    wheels = DockerOperator(
        task_id="process_unfinished_wheels",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./provider_runtool.py --provider 'wheels' --time-min '{time_min}' --time-max '{time_max}' --incomplete-only --no-logs",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=environment_vars,
    )

    # Task: process_unfinished_spin
    # Description: Processes unfinished schedule blocks assigned to Spin
    spin = DockerOperator(
        task_id="process_unfinished_spin",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./provider_runtool.py --provider 'spin' --time-min '{time_min}' --time-max '{time_max}' --incomplete-only --no-logs",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=environment_vars,
    )

    # Task: process_unfinished_ojo
    # Description: Processes unfinished schedule blocks assigned to Ojo
    ojo = DockerOperator(
        task_id="process_unfinished_ojo",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./provider_runtool.py --provider 'ojo' --time-min '{time_min}' --time-max '{time_max}' --incomplete-only --no-logs",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=environment_vars,
    )

    revel = DockerOperator(
        task_id="process_unfinished_revel",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./provider_runtool.py --provider 'revel' --time-min '{time_min}' --time-max '{time_max}' --incomplete-only --no-logs",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=environment_vars,
    )

    # lyft >> \
    # jump >> \
    lime >> \
    bird >> \
    wheels >> \
    spin >> \
    ojo >> \
    revel
