from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from _slack_operators import task_fail_slack_alert

"""We're handling 5 datasets here, all of which land in an AGOL feature class,
here: https://austin.maps.arcgis.com/home/item.html?id=a9f5be763a67442a98f684935d15729b
"""

default_args = {
    "owner": "airflow",
    "description": "Loads pavement markings work orders from Knack to AGOL",
    "depend_on_past": False,
    "start_date": datetime(2020, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-knack-services:production"

# command args
script_task_1 = "records_to_postgrest"
script_task_2 = "records_to_agol"
script_task_3 = "agol_build_markings_segment_geometries"
app_name = "signs-markings"
env = "prod"

# assemble env vars
env_vars = Variable.get("atd_knack_services_postgrest", deserialize_json=True)
atd_knack_auth = Variable.get("atd_knack_auth", deserialize_json=True)
env_vars["KNACK_APP_ID"] = atd_knack_auth[app_name][env]["app_id"]
env_vars["KNACK_API_KEY"] = atd_knack_auth[app_name][env]["api_key"]
env_vars["AGOL_USERNAME"] = Variable.get("agol_username")
env_vars["AGOL_PASSWORD"] = Variable.get("agol_password")

with DAG(
    dag_id="atd_knack_markings_work_orders",
    default_args=default_args,
    schedule_interval="30 4 * * *",
    dagrun_timeout=timedelta(minutes=60),
    tags=["production", "knack"],
    catchup=False,
) as dag:
    # completely replace data on 15th day of every month
    # this is a failsafe catch records that may have been missed via incremental loading
    date_filter = "{{ '1970-01-01' if ds.endswith('15') else prev_execution_date_success or '1970-01-01' }}"  # noqa:E501

    # work orders
    container = "view_3099"
    t1 = DockerOperator(
        task_id="atd_knack_markings_work_orders_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_1}.py -a {app_name} -c {container} -d "{date_filter}"',  # noqa:E501
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t2 = DockerOperator(
        task_id="atd_knack_markings_work_orders_to_agol",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_2}.py -a {app_name} -c {container} -d "{date_filter}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t3 = DockerOperator(
        task_id="atd_knack_build_agol_geometries",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_3}.py -l markings_work_orders -d "{date_filter}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    # markings jobs
    container = "view_3100"
    t4 = DockerOperator(
        task_id="atd_knack_markings_jobs_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_1}.py -a {app_name} -c {container} -d "{date_filter}"',  # noqa:E501
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t5 = DockerOperator(
        task_id="atd_knack_markings_jobs_to_agol",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_2}.py -a {app_name} -c {container} -d "{date_filter}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t6 = DockerOperator(
        task_id="atd_knack_build_agol_geometries",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_3}.py -l markings_jobs -d "{date_filter}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    # attachments
    container = "view_3096"
    t7 = DockerOperator(
        task_id="atd_knack_markings_attachments_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_1}.py -a {app_name} -c {container} -d "{date_filter}"',  # noqa:E501
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t8 = DockerOperator(
        task_id="atd_knack_markings_attachments_to_agol",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_2}.py -a {app_name} -c {container} -d "{date_filter}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    # specification actuals
    container = "view_3103"
    t9 = DockerOperator(
        task_id="atd_knack_markings_spec_actuals_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_1}.py -a {app_name} -c {container} -d "{date_filter}"',  # noqa:E501
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t10 = DockerOperator(
        task_id="atd_knack_markings_spec_actuals_to_agol",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_2}.py -a {app_name} -c {container} -d "{date_filter}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    # materials
    container = "view_3104"
    t11 = DockerOperator(
        task_id="atd_knack_markings_materials_to_postgrest",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_1}.py -a {app_name} -c {container} -d "{date_filter}"',  # noqa:E501
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t12 = DockerOperator(
        task_id="atd_knack_markings_materials_to_agol",
        image=docker_image,
        api_version="auto",
        auto_remove=True,
        command=f'./atd-knack-services/services/{script_task_2}.py -a {app_name} -c {container} -d "{date_filter}"',  # noqa
        docker_url="tcp://localhost:2376",
        network_mode="bridge",
        environment=env_vars,
        tty=True,
    )

    t1 >> t2 >> t3  # work orders
    t4 >> t5 >> t6  # jobs
    t7 >> t8  # attachments
    t9 >> t10  # specifications
    t11 >> t12  # materials


if __name__ == "__main__":
    dag.cli()
