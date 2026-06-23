from docker.errors import APIError
from requests.exceptions import RequestException

from airflow.exceptions import AirflowException
from airflow.providers.docker.operators.docker import DockerOperator


class DockerOperatorWithFallback(DockerOperator):
    """DockerOperator that attempts force_pull but falls back to the cached image on pull failure.

    Useful when force_pull=True is desired for freshness but a registry outage should not
    prevent the task from running with the last successfully pulled image.

    If the pull fails and no local image exists, the task fails as normal.
    """

    def execute(self, context):
        if self.force_pull or not self.cli.images(name=self.image):
            self.log.info("::group::Pulling docker image %s", self.image)
            pull_error: str | None = None
            try:
                latest_status: dict[str, str] = {}
                for output in self.cli.pull(self.image, stream=True, decode=True):
                    if isinstance(output, str):
                        self.log.info("%s", output)
                        continue
                    if isinstance(output, dict):
                        if "error" in output:
                            pull_error = output["error"]
                            break
                        if "status" in output:
                            output_status = output["status"]
                            if "id" not in output:
                                self.log.info("%s", output_status)
                                continue
                            output_id = output["id"]
                            if latest_status.get(output_id) != output_status:
                                self.log.info("%s: %s", output_id, output_status)
                                latest_status[output_id] = output_status
            except (APIError, RequestException) as e:
                pull_error = str(e)
            self.log.info("::endgroup::")
            if pull_error:
                if self.cli.images(name=self.image):
                    self.log.warning(
                        "Pull failed for %s: %s. Running with cached image.", self.image, pull_error
                    )
                else:
                    raise AirflowException(
                        f"Pull failed for {self.image} and no cached image exists: {pull_error}"
                    )
        return self._run_image()
