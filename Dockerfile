FROM apache/airflow:2.5.3

USER root
RUN apt-get update
RUN apt-get install -y aptitude magic-wormhole vim black 

# install zip archive tool for CRIS export use
RUN apt-get install -y p7zip-full

# bastion host support for local dev
RUN apt-get install -y openssh-server
# ssh needs this folder for connection tracking
RUN mkdir /run/sshd
# add ssh daemon to entrypoint
RUN sed -i '2i/usr/sbin/sshd' /entrypoint
# generate a keypair for ssh
RUN ssh-keygen -t ed25519 -C "airflow@airflow" -f /root/.ssh/id_ed25519 -q -N ""
# allow ssh to localhost
RUN cat /root/.ssh/id_ed25519.pub > /root/.ssh/authorized_keys
# disable the "are you sure you want to connect" prompt
RUN printf "Host localhost\n\tStrictHostKeyChecking no\n" >> /root/.ssh/config


USER ${AIRFLOW_UID}
COPY airflow.cfg /opt/airflow/airflow.cfg
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install -r /opt/airflow/requirements.txt
