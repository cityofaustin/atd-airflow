FROM apache/airflow:2.6.1

USER root
RUN apt-get update
RUN apt-get install -y aptitude magic-wormhole vim black awscli

USER ${AIRFLOW_UID}
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install -r /opt/airflow/requirements.txt
