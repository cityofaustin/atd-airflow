from __future__ import annotations

from docker.errors import APIError
from requests.exceptions import RequestException

from airflow.exceptions import AirflowException
from airflow.providers.docker.operators.docker import DockerOperator


# Below are substrings that identify a pull failure as a temporary infrastructure problem —
# a registry outage, network hiccup, or TCP-level failure — rather than a
# configuration or authentication error. Only errors that match one of these patterns
# will trigger the cached-image fallback. Everything else (4xx auth/not-found errors,
# or anything we don't recognize) is re-raised so the task fails visibly.
#
# WHY a whitelist instead of a blacklist of 4xx patterns:
#   The Docker daemon does not include the raw HTTP status code in streamed error dicts.
#   It only gives us an error message string. Whitelisting known transient patterns is
#   safer than trying to blacklist every possible 4xx message phrasing — unrecognized
#   errors default to a hard failure rather than a silent fallback.
_TRANSIENT_PULL_ERROR_PATTERNS = (
    "dial tcp",                  # TCP connection failure (refused, unreachable, etc.)
    "i/o timeout",               # read/write timeout on the socket
    "context deadline exceeded",  # overall request deadline hit (daemon-level timeout)
    "no such host",              # DNS resolution failed
    " EOF",                      # connection dropped mid-stream; leading space avoids matching
                                 # "errorDetail.EOF" or similar field names in error dicts
    "TLS handshake timeout",     # TLS negotiation timed out (often precedes a connection drop)
    "500 Internal Server Error",
    "502 Bad Gateway",
    "503 Service Unavailable",
    "504 Gateway Timeout",
)


def _is_transient_pull_error(error: str) -> bool:
    """Return True if the error string indicates a temporary infrastructure failure.

    Returns False for anything that doesn't match a known transient pattern, which
    causes the caller to re-raise rather than fall back. This means 4xx errors
    (wrong image name, expired credentials, unauthorized) always fail loudly.
    """
    return any(pattern in error for pattern in _TRANSIENT_PULL_ERROR_PATTERNS)


class DockerOperatorWithFallback(DockerOperator):
    """DockerOperator that attempts force_pull but falls back to the cached image on transient pull failures.

    The standard DockerOperator with force_pull=True will fail the entire task if the registry
    is unreachable, even when a perfectly usable image is already cached on the host. This
    wrapper changes that behavior for temporary infrastructure failures: it tries to pull, and
    if the pull fails due to a network outage or registry 5xx, it logs a warning and runs the
    cached image instead.

    NOT all pull failures trigger the fallback. 4xx errors (wrong image tag, expired credentials,
    unauthorized access) are re-raised immediately so they don't silently mask misconfigurations.
    Any error string that doesn't match a known transient pattern is also re-raised by default.

    There is one additional case where a transient failure still causes the task to fail: when
    there is no cached image on the host. In that case there is nothing to fall back to.

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
            # the error text here so both failure paths share the same classification logic below.
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
                        # IMPORTANT: When the registry is unreachable or returns an error,
                        # the Docker daemon does NOT raise a Python exception. Instead it
                        # streams back a JSON object with an "error" key, for example:
                        #
                        #   4xx: {"error": "manifest for python:bad-tag not found: manifest unknown"}
                        #   4xx: {"error": "pull access denied: repository does not exist or may require 'docker login'"}
                        #   5xx: {"error": "Get \"https://registry-1.docker.io/v2/\": unexpected status code 503 Service Unavailable"}
                        #  TCP: {"error": "Get \"https://registry-1.docker.io/v2/\": dial tcp 127.0.0.1:443: connect: connection refused"}
                        #
                        # If we don't check for this key, the loop completes silently and
                        # the container runs from cache with no log entry — making it
                        # impossible to tell whether the pull succeeded or was skipped.
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
                # True Python exceptions can still occur in some failure modes. For example:
                #   - RequestException: network error talking to the Docker daemon socket itself
                #     (distinct from the daemon talking to the registry). Always transient.
                #   - APIError: the daemon rejected the pull request at the API level.
                #     Apply the same pattern check as streamed error dicts.
                pull_error = str(e)

            # Close the log group regardless of whether the pull succeeded or failed,
            # so the UI doesn't leave an unclosed collapsible section. Everything below
            # is intentionally placed AFTER ::endgroup:: so it is always visible in the
            # logs and never hidden inside the collapsed pull output section.
            self.log.info("::endgroup::")

            if pull_error:
                if not _is_transient_pull_error(pull_error):
                    # The error doesn't match any known transient pattern — treat it as a
                    # hard failure. This covers 4xx errors (wrong image name, bad credentials,
                    # unauthorized) and anything else we don't recognize. We never silently
                    # fall back for these, even if a cached image happens to exist, because
                    # that would mask real misconfigurations.
                    raise AirflowException(
                        f"Pull failed for {self.image} with a non-retryable error "
                        f"(check image name and credentials): {pull_error}"
                    )

                # The error is a known transient failure (network outage, 5xx). Now check
                # whether we actually have something to fall back to.
                if self.cli.images(name=self.image):
                    # A cached image exists — we can run it. Log a prominent warning so
                    # it's obvious in the task logs that the pull failed and the cached
                    # image was used instead of a freshly pulled one.
                    self.log.warning(
                        "Pull failed for %s (transient): %s. Running with cached image.",
                        self.image,
                        pull_error,
                    )
                else:
                    # Transient failure AND no cached image — there is nothing to run.
                    raise AirflowException(
                        f"Pull failed for {self.image} (transient) and no cached image "
                        f"exists to fall back to: {pull_error}"
                    )

        # Delegate container creation and execution entirely to the parent. Nothing about
        # how the container actually runs is changed by this wrapper.
        return self._run_image()
