from __future__ import annotations

from functools import cached_property

from airflow.providers.docker.hooks.docker import DockerHook
from airflow.providers.docker.operators.docker import DockerOperator


class _DockerHookWithLoginFallback(DockerHook):
    """DockerHook that warns instead of raising when registry login fails."""

    _BASE_LOGIN_METHOD = "_DockerHook__login"

    @classmethod
    def _require_base_login_method(cls):
        method = getattr(DockerHook, cls._BASE_LOGIN_METHOD, None)
        if not callable(method):
            raise RuntimeError(
                "Docker provider internals changed: "
                f"`DockerHook.{cls._BASE_LOGIN_METHOD}` is missing or not callable. "
                "Update _DockerHookWithLoginFallback to match the provider implementation."
            )
        return method

    def _DockerHook__login(self, client, conn):
        base_login = self._require_base_login_method()
        try:
            base_login(self, client, conn)
        except Exception as e:
            status_code = getattr(e, "status_code", None)
            if status_code is None:
                response = getattr(e, "response", None)
                status_code = getattr(response, "status_code", None)

            if not isinstance(status_code, int) or status_code < 500 or status_code >= 600:
                raise

            self.log.warning(
                "Registry login failed with server error %s (%s). Proceeding without authentication.",
                status_code,
                e,
            )


class DockerOperatorWithFallback(DockerOperator):
    """
    DockerOperator that attempts a force pull but falls back to a local image on failure.
    All other functionality is the same as DockerOperator, `force_pull` is ignored.
    """

    @staticmethod
    def _extract_status_code(exc: Exception):
        status_code = getattr(exc, "status_code", None)
        if status_code is not None:
            return status_code

        response = getattr(exc, "response", None)
        return getattr(response, "status_code", None)

    @classmethod
    def _should_fallback_to_local_image(cls, exc: Exception) -> bool:
        status_code = cls._extract_status_code(exc)
        if isinstance(status_code, int):
            return 500 <= status_code < 600

        if isinstance(exc, (TimeoutError, ConnectionError)):
            return True

        return False

    @cached_property
    def hook(self) -> _DockerHookWithLoginFallback:
        """
        Original Method:https://github.com/apache/airflow/blob/dc939331f4cd90892fd201931c466dffff977a4f/providers/docker/src/airflow/providers/docker/operators/docker.py#L343
        """
        
        tls_config = DockerHook.construct_tls_config(
            ca_cert=self.tls_ca_cert,
            client_cert=self.tls_client_cert,
            client_key=self.tls_client_key,
            verify=self.tls_verify,
            assert_hostname=self.tls_hostname,
            ssl_version=self.tls_ssl_version,
        )
        return _DockerHookWithLoginFallback(
            docker_conn_id=self.docker_conn_id,
            base_url=self.docker_url,
            version=self.api_version,
            tls=tls_config,
            timeout=self.timeout,
        )

    def execute(self, context):
        """
        Original Method: https://github.com/apache/airflow/blob/dc939331f4cd90892fd201931c466dffff977a4f/providers/docker/src/airflow/providers/docker/operators/docker.py#L487
        """

        self.log.info("Attempting to pull image %s", self.image)
        try:
            for output in self.cli.pull(self.image, stream=True, decode=True):
                if isinstance(output, dict) and "status" in output:
                    self.log.info("%s", output.get("status", ""))
        except Exception as e:
            if not self._should_fallback_to_local_image(e):
                raise

            self.log.warning(
                "Could not pull image %s (%s). Using local image if available.",
                self.image,
                e,
            )
        return self._run_image()
