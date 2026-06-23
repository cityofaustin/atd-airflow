from docker.errors import APIError
from requests.exceptions import RequestException

from airflow.exceptions import AirflowException
from airflow.providers.docker.operators.docker import DockerOperator


class DockerOperatorWithFallback(DockerOperator):
    """DockerOperator that attempts force_pull but falls back to the cached image on pull failure.

    The standard DockerOperator with force_pull=True will fail the entire task if the registry
    is unreachable, even when a perfectly usable image is already cached on the host. This
    wrapper changes that behavior: it tries to pull, and if the pull fails for any reason
    (registry outage, DNS failure, timeout, etc.), it logs a warning and runs the cached image
    instead.

    There is one case where a pull failure still causes the task to fail: when force_pull=True
    but there is no cached image on the host yet. In that case there is nothing to fall back to,
    so failing is the correct behavior.

    Usage is identical to DockerOperator — just swap the class name. All other parameters,
    behavior, and inherited methods (xcom, auto_remove, on_kill, etc.) are unchanged.
    """

    def execute(self, context):
        # Replicate the parent's condition for deciding whether to attempt a pull:
        #   - force_pull=True means "always try to pull the latest from the registry"
        #   - if the image isn't cached locally at all, we must try to pull regardless
        if self.force_pull or not self.cli.images(name=self.image):

            # ::group:: / ::endgroup:: are Airflow log-grouping markers that collapse the
            # verbose layer-by-layer pull output in the UI, matching the parent's behavior.
            self.log.info("::group::Pulling docker image %s", self.image)

            # pull_error stays None if the pull succeeds. If anything goes wrong — whether
            # an exception is raised or Docker streams back an error message — we capture
            # the error text here so both failure paths can share the same fallback logic below.
            pull_error: str | None = None

            try:
                # cli.pull() with stream=True and decode=True returns a generator that yields
                # JSON-decoded progress dicts as the Docker daemon streams them. The actual
                # network connection to the registry happens lazily as we iterate — meaning
                # registry errors surface during the loop, not at the cli.pull() call itself.
                latest_status: dict[str, str] = {}
                for output in self.cli.pull(self.image, stream=True, decode=True):

                    if isinstance(output, str):
                        # Occasionally the daemon yields a plain string instead of a dict.
                        self.log.info("%s", output)
                        continue

                    if isinstance(output, dict):
                        # IMPORTANT: When the registry is unreachable, the Docker daemon does
                        # NOT raise a Python exception. Instead it streams back a JSON object
                        # with an "error" key, e.g.:
                        #   {"error": "Get \"https://registry-1.docker.io/v2/\": dial tcp ..."}
                        # If we don't check for this key, the loop completes silently and the
                        # container runs from cache without any log entry — making it impossible
                        # to tell whether the pull actually succeeded or was silently skipped.
                        if "error" in output:
                            pull_error = output["error"]
                            break

                        if "status" in output:
                            # Normal progress message. Each image layer reports status updates
                            # like "Pulling fs layer", "Downloading", "Pull complete", etc.
                            # We deduplicate by (layer id, status) to avoid spamming the log
                            # with repeated lines for the same layer state.
                            output_status = output["status"]
                            if "id" not in output:
                                # Status line with no layer id (e.g. "Pulling from library/python")
                                self.log.info("%s", output_status)
                                continue
                            output_id = output["id"]
                            if latest_status.get(output_id) != output_status:
                                self.log.info("%s: %s", output_id, output_status)
                                latest_status[output_id] = output_status

            except (APIError, RequestException) as e:
                # True Python exceptions can still occur in some failure modes — for example,
                # if the Docker daemon itself is unreachable (socket error) rather than the
                # registry being unreachable. Capture the message so it flows through the
                # same fallback logic as the streamed-error-dict case above.
                pull_error = str(e)

            # Close the log group regardless of whether the pull succeeded or failed,
            # so the UI doesn't leave an unclosed collapsible section. The warning below
            # is intentionally placed AFTER ::endgroup:: so it is always visible in the
            # logs and never hidden inside the collapsed pull output section.
            self.log.info("::endgroup::")

            if pull_error:
                if self.cli.images(name=self.image):
                    # A cached image exists — we can run it. Log a prominent warning so
                    # it's obvious in the task logs that the pull failed and the cached
                    # image was used instead of a freshly pulled one.
                    self.log.warning(
                        "Pull failed for %s: %s. Running with cached image.", self.image, pull_error
                    )
                else:
                    # No cached image and the pull failed — there is nothing to run.
                    # Fail the task with a clear message rather than letting Docker raise
                    # a cryptic "image not found" error from _run_image().
                    raise AirflowException(
                        f"Pull failed for {self.image} and no cached image exists: {pull_error}"
                    )

        # Delegate container creation and execution entirely to the parent. Nothing about
        # how the container actually runs is changed by this wrapper.
        return self._run_image()
