from __future__ import annotations

from airflow.providers.docker.operators.docker import DockerOperator


class DockerOperatorWithFallback(DockerOperator):
    """DockerOperator that attempts a force pull but falls back to a local image on failure."""

    def execute(self, context):
        original_force_pull = getattr(self, "force_pull", False)
        self.log.info("Attempting to pull image %s", self.image)
        try:
            self.force_pull = True
            return super().execute(context)
        except Exception as e:
            self.log.warning(
                "Could not pull image %s (%s). Using local image if available.",
                self.image,
                e,
            )
            self.force_pull = False
            return super().execute(context)
        finally:
            self.force_pull = original_force_pull
