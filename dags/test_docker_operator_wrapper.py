from airflow.sdk import DAG
from pendulum import datetime, duration

from utils.docker_operator_with_fallback import DockerOperatorWithFallback

doc_md = """
## DockerOperatorWithFallback test DAG

Validates the `DockerOperatorWithFallback` wrapper in `utils/docker_operator_with_fallback.py`.

### Tasks

- **pull_succeeds**: normal happy path — `force_pull=True`, Docker Hub is reachable.
  The image is pulled fresh and the container runs `echo`.

### Testing the fallback path

To exercise the "Hub is down, use cache" behavior without actually taking Hub offline:

1. Make sure `python:3-slim` is already cached locally:
   ```
   docker pull python:3-slim
   ```
2. Add a `/etc/hosts` entry to make the registry unreachable:
   ```
   sudo sh -c 'echo "127.0.0.1 registry-1.docker.io" >> /etc/hosts'
   ```
3. Trigger this DAG manually from the Airflow UI.
4. Confirm the task logs contain a `WARNING: Failed to pull image … Running with cached image.`
   line and that the task still succeeds.
5. Remove the hosts entry when done:
   ```
   sudo sed -i '' '/registry-1.docker.io/d' /etc/hosts
   ```

### Testing the "no cache + hub down" failure path

With the `/etc/hosts` block in place:
1. Remove the local image: `docker rmi python:3-slim`
2. Trigger the DAG — the task should **fail** because there is no cached image to fall back to.
"""

with DAG(
    dag_id="test_docker_operator_wrapper",
    description="Manual smoke test for DockerOperatorWithFallback",
    start_date=datetime(2024, 1, 1, tz="America/Chicago"),
    schedule=None,
    catchup=False,
    tags=["test"],
    doc_md=doc_md,
) as dag:
    DockerOperatorWithFallback(
        task_id="pull_succeeds",
        image="python:3-slim",
        command=["python", "-c", "print('DockerOperatorWithFallback: OK')"],
        force_pull=True,
        auto_remove="force",
        execution_timeout=duration(minutes=5),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )
