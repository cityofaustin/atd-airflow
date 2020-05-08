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
    "on_failure_callback"   : task_fail_slack_alert,
}


current_time_min = datetime.now()
current_time_max = datetime.now() + timedelta(days=-1, hours=-8)
time_min = f"{current_time_min.year}-{current_time_min.month}-01-01"
time_max = f"{current_time_max.year}-{current_time_max.month}-{current_time_max.day}-{current_time_max.hour}"
environment_vars = Variable.get("atd_mds_config_staging", deserialize_json=True)
docker_image = "atddocker/atd-mds-etl:master"

with DAG(
    f"atd_mds_process_unfinished_staging",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["staging", "mds"],
) as dag:
    # Task: process_unfinished_lime
    # Description: Processes unfinished schedule blocks assigned to Lime
    lime = DockerOperator(
        task_id="process_unfinished_lime",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f"./provider_runtool.py --provider 'lime' --time-max '{time_max}' --time-min '{time_min}' --incomplete-only --no-logs",
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
        command=f"./provider_runtool.py --provider 'jump' --time-max '{time_max}' --time-min '{time_min}' --incomplete-only --no-logs",
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
        command=f"./provider_runtool.py --provider 'bird' --time-max '{time_max}' --time-min '{time_min}' --incomplete-only --no-logs",
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
        command=f"./provider_runtool.py --provider 'lyft' --time-max '{time_max}' --time-min '{time_min}' --incomplete-only --no-logs",
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
        command=f"./provider_runtool.py --provider 'wheels' --time-max '{time_max}' --time-min '{time_min}' --incomplete-only --no-logs",
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
        command=f"./provider_runtool.py --provider 'spin' --time-max '{time_max}' --time-min '{time_min}' --incomplete-only --no-logs",
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
        command=f"./provider_runtool.py --provider 'ojo' --time-max '{time_max}' --time-min '{time_min}' --incomplete-only --no-logs",
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=environment_vars,
    )

    jump >> lime >> bird >> lyft >> wheels >> spin >> ojo
