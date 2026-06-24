import docker
import requests
from airflow.sdk import DAG, task
from pendulum import datetime, duration

from utils.docker_operator_with_fallback import DockerOperatorWithFallback

_TEST_IMAGE = "python:3-slim"

doc_md = """
## DockerOperatorWithFallback test DAG

Hint: This documentation is much easier to read by closing this modal and examining the code tab below.

---

Validates all failure modes of `DockerOperatorWithFallback`
(`dags/utils/docker_operator_with_fallback.py`).

Trigger this DAG manually with no configuration. Two setup tasks automatically detect
whether the test image is cached locally and whether Docker Hub is reachable, then route
to only the tasks that are meaningful given the current environment.

---

## Which tasks run

| Registry reachable? | Image cached locally? | Tasks that run |
|---|---|---|
| Yes | (either) | `happy_path`, `non_retryable_4xx` |
| No | Yes | `transient_failure_with_cache` |
| No | No | `transient_failure_no_cache` |

---

## Simulating an outage

To force the registry-unreachable scenarios, block Docker Hub via `/etc/hosts`:

```
sudo sh -c 'echo "127.0.0.1 registry-1.docker.io" >> /etc/hosts'
```

Restore when done:

```
sudo sed -i '' '/registry-1.docker.io/d' /etc/hosts
```

To also test `transient_failure_no_cache`, remove the local image before triggering:

```
docker rmi python:3-slim
```

---

## Expected outcomes

**`happy_path`** — Pull succeeds, container runs.
Pull progress lines appear in the collapsed log group. Container prints `DockerOperatorWithFallback: OK`.

**`non_retryable_4xx`** — Tag does not exist on Docker Hub. Task fails quickly with:
```
AirflowException: Pull failed for python:this-tag-does-not-exist with a non-retryable error
(check image name and credentials): manifest for python:this-tag-does-not-exist not found ...
```

**`transient_failure_with_cache`** — Pull times out after ~1 minute. Task succeeds with a visible warning:
```
WARNING - Pull failed for python:3-slim (transient): ... dial tcp ... Running with cached image.
```
Container prints `Running from cached image after transient pull failure`.

**`transient_failure_no_cache`** — Pull times out after ~1 minute. Task fails with:
```
AirflowException: Pull failed for python:3-slim (transient) and no cached image exists to fall back to: ...
```
"""

_COMMON_KWARGS = dict(
    force_pull=True,
    auto_remove="force",
    execution_timeout=duration(minutes=5),
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
)

with DAG(
    dag_id="test_docker_operator_wrapper",
    description="Manual smoke test for DockerOperatorWithFallback",
    start_date=datetime(2024, 1, 1, tz="America/Chicago"),
    schedule=None,
    catchup=False,
    tags=["test"],
    doc_md=doc_md,
) as dag:

    @task
    def check_image_cached() -> bool:
        """Return True if the test image is already present in the local Docker image cache."""
        client = docker.APIClient(base_url="unix://var/run/docker.sock")
        return bool(client.images(name=_TEST_IMAGE))

    @task
    def check_registry_reachable() -> bool:
        """Return True if Docker Hub's registry API is reachable.

        Probes the v2 API endpoint directly with a short timeout. A 401 response means
        the registry is up but unauthenticated (normal for an anonymous probe). Any 5xx,
        connection error, DNS failure, or timeout is treated as unreachable.

        Using a 5-second timeout here is important: without it, a real network outage
        would block this probe for the same ~1 minute as the actual pull, defeating the
        purpose of detecting the state up front.
        """
        try:
            resp = requests.get("https://registry-1.docker.io/v2/", timeout=5)
            # 401 = registry is up, just unauthenticated (expected for anonymous probe)
            # 2xx or other 4xx = registry is reachable
            # 5xx = registry is having trouble, treat same as unreachable
            return resp.status_code < 500
        except requests.exceptions.RequestException:
            # DNS failure, connection refused, timeout — registry is unreachable
            return False

    @task.branch(task_id="select_tests")
    def select_tests(image_cached: bool, registry_reachable: bool) -> list[str]:
        """Return the task ids that are valid to run given the current environment."""
        if registry_reachable:
            # Hub is reachable: test the happy path and the 4xx hard-failure behavior.
            # The 4xx task needs a live registry to actually return a 404 — if the registry
            # were unreachable, that error would look like a transient TCP failure instead.
            return ["happy_path", "non_retryable_4xx"]

        if image_cached:
            # Hub is unreachable and the image is cached: the wrapper should fall back gracefully.
            return ["transient_failure_with_cache"]

        # Hub is unreachable and the image is NOT cached: nothing to fall back to, must fail.
        return ["transient_failure_no_cache"]

    cached = check_image_cached()
    reachable = check_registry_reachable()
    branch = select_tests(cached, reachable)

    happy_path = DockerOperatorWithFallback(
        task_id="happy_path",
        image=_TEST_IMAGE,
        command=["python", "-c", "print('DockerOperatorWithFallback: OK')"],
        **_COMMON_KWARGS,
    )

    transient_failure_with_cache = DockerOperatorWithFallback(
        task_id="transient_failure_with_cache",
        image=_TEST_IMAGE,
        command=["python", "-c", "print('Running from cached image after transient pull failure')"],
        **_COMMON_KWARGS,
    )

    transient_failure_no_cache = DockerOperatorWithFallback(
        task_id="transient_failure_no_cache",
        image=_TEST_IMAGE,
        command=["python", "-c", "print('This should never run, transient_failure_no_cache')"],
        **_COMMON_KWARGS,
    )

    # Intentionally nonexistent tag. With a live registry this gets a 404, which the
    # wrapper must re-raise rather than fall back on.
    non_retryable_4xx = DockerOperatorWithFallback(
        task_id="non_retryable_4xx",
        image="python:this-tag-does-not-exist",
        command=["python", "-c", "print('This should never run, non_retryable_4xx')"],
        **_COMMON_KWARGS,
    )

    branch >> [happy_path, transient_failure_with_cache, transient_failure_no_cache, non_retryable_4xx]
