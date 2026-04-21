from __future__ import annotations

from airflow.providers.docker.operators.docker import DockerOperator


class DockerOperatorWithFallback(DockerOperator):
    """DockerOperator that attempts a force pull but falls back to a local image on failure."""

    def execute(self, context):
        self.log.info("Attempting to pull image %s", self.image)
        try:
            for output in self.cli.pull(self.image, stream=True, decode=True):
                if isinstance(output, dict) and "status" in output:
                    self.log.info("%s", output.get("status", ""))
        except Exception as e:
            self.log.warning(
                "Could not pull image %s (%s). Using local image if available.",
                self.image,
                e,
            )
        return self._run_image()
