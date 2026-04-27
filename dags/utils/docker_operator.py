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
            self.log.warning(
                "Registry login failed (%s). Proceeding without authentication.", e
            )


class DockerOperatorWithFallback(DockerOperator):
    """DockerOperator that attempts a force pull but falls back to a local image on failure."""

    @cached_property
    def hook(self) -> _DockerHookWithLoginFallback:
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
