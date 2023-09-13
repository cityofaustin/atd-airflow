# test locally with: docker compose run --rm airflow-cli dags test atd_parking_data
import os
from datetime import timedelta

from airflow.decorators import task
from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.operators.docker_operator import DockerOperator
from pendulum import datetime, duration

from utils.onepassword import get_env_vars_task
from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": duration(minutes=60 * 5),
    "on_failure_callback": task_fail_slack_alert,
}

docker_image = "atddocker/atd-parking-data-meters:production"

REQUIRED_SECRETS = {
    # Socrata
    "SO_WEB": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.endpoint",
    },
    "SO_PASS": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeySecret",
    },
    "SO_USER": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.apiKeyId",
    },
    "SO_TOKEN": {
        "opitem": "Socrata Key ID, Secret, and Token",
        "opfield": "socrata.appToken",
    },
    "TXNS_DATASET": {
        "opitem": "Parking Data ETL",
        "opfield": "socrata.Transactions Dataset",
    },
    "FISERV_DATASET": {
        "opitem": "Parking Data ETL",
        "opfield": "socrata.Fiserv Payments Dataset",
    },
    "METERS_DATASET": {
        "opitem": "Parking Data ETL",
        "opfield": "socrata.Parking Meters Dataset",
    },
    "PAYMENTS_DATASET": {
        "opitem": "Parking Data ETL",
        "opfield": "socrata.Parking Meter Credit Card Payments Dataset",
    },
    # PostgREST
    "POSTGREST_TOKEN": {
        "opitem": "Parking Data ETL",
        "opfield": "postgrest.Token",
    },
    "POSTGREST_ENDPOINT": {
        "opitem": "Parking Data ETL",
        "opfield": "postgrest.Endpoint",
    },
    # Passport
    "OPS_MAN_USER": {
        "opitem": "Parking Data ETL",
        "opfield": "passport.Username",
    },
    "OPS_MAN_PASS": {
        "opitem": "Parking Data ETL",
        "opfield": "passport.Password",
    },
    # Fiserv
    "FSRV_EMAIL": {
        "opitem": "Parking Data ETL",
        "opfield": "fiserv.Expected Email Address",
    },
    "ENCRYPTION_KEY": {
        "opitem": "Parking Data ETL",
        "opfield": "fiserv.Encryption Key",
    },
    # AWS S3
    "AWS_ACCESS_ID": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Access ID",
    },
    "AWS_ACCESS_ID": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Access ID",
    },
    "AWS_ACCESS_KEY_ID": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Access ID",
    },
    "AWS_SECRET_ACCESS_KEY": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Secret Access Key",
    },
    "AWS_PASS": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Secret Access Key",
    },
    "BUCKET": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Bucket Name",
    },
    "BUCKET_NAME": {
        "opitem": "Parking Data ETL",
        "opfield": "aws.Bucket Name",
    },
    # Flowbird
    "ENDPOINT": {
        "opitem": "Parking Data ETL",
        "opfield": "flowbird.Endpoint",
    },
    "USER": {
        "opitem": "Parking Data ETL",
        "opfield": "flowbird.ATD Username",
    },
    "PASSWORD": {
        "opitem": "Parking Data ETL",
        "opfield": "flowbird.ATD Password",
    },
    "USER_PARD": {
        "opitem": "Parking Data ETL",
        "opfield": "flowbird.PARD Username",
    },
    "PASSWORD_PARD": {
        "opitem": "Parking Data ETL",
        "opfield": "flowbird.PARD Password",
    },
}


with DAG(
    dag_id="atd_parking_data",
    description="Scripts that download and process parking data for finance reporting.",
    default_args=default_args,
    schedule_interval="35 8 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    tags=["repo:atd-parking-data", "parking", "socrata", "postgrest"],
    catchup=False,
) as dag:
    env_vars = get_env_vars_task(REQUIRED_SECRETS)
    prev_exec = "{{ (prev_start_date_success - macros.timedelta(days=7)).strftime('%Y-%m-%d') if prev_start_date_success else '2023-08-28'}}"

    docker_tasks = []

    docker_tasks.append(
        DockerOperator(
            task_id="smartfolio_transactions",
            image=docker_image,
            command=f"python txn_history.py -v --report transactions --start {prev_exec}",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=True,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="smartfolio_payments",
            image=docker_image,
            command=f"python txn_history.py -v --report payments --start {prev_exec}",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="smartfolio_pard_payments",
            image=docker_image,
            command=f"python txn_history.py -v --report payments --user pard --start {prev_exec}",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="passport_transactions",
            image=docker_image,
            command=f"python passport_txns.py -v --start {prev_exec}",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="process_fiserv_emails",
            image=docker_image,
            command=f"python fiserv_email_pub.py",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="process_fiserv_attachments",
            image=docker_image,
            command=f"python fiserv_DB.py --lastmonth True",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="process_smartfolio_payments",
            image=docker_image,
            command=f"python payments_s3.py --lastmonth True",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="process_pard_smartfolio_payments",
            image=docker_image,
            command=f"python payments_s3.py --user pard --lastmonth True",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="process_passport_transactions",
            image=docker_image,
            command=f"python passport_DB.py --lastmonth True",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="process_smartfolio_transactions",
            image=docker_image,
            command=f"python smartfolio_s3.py --lastmonth True",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="match_fiserv_and_smartfolio_payments",
            image=docker_image,
            command=f"python match_field_processing.py",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="payments_to_socrata",
            image=docker_image,
            command=f"python parking_socrata.py --dataset payments",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="fiserv_to_socrata",
            image=docker_image,
            command=f"python parking_socrata.py --dataset fiserv",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    docker_tasks.append(
        DockerOperator(
            task_id="transactions_to_socrata",
            image=docker_image,
            command=f"python parking_socrata.py --dataset transactions",
            api_version="auto",
            auto_remove=True,
            environment=env_vars,
            tty=True,
            force_pull=False,
            retries=3,
            retry_delay=duration(seconds=60),
        )
    )

    # All tasks will run sequentially
    for i in range(1, len(docker_tasks)):
        docker_tasks[i - 1] >> docker_tasks[i]
