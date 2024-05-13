FROM apache/airflow:2.9.0

USER root
RUN apt-get update
RUN apt-get install -y aptitude magic-wormhole vim black awscli gosu

USER ${AIRFLOW_UID}
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install -r /opt/airflow/requirements.txt
