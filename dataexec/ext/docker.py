import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import docker
from pydantic import BaseModel

logger = logging.getLogger("docker")


class DockerBuildLowLog(BaseModel):
    logs: str
    error: bool


class DockerPushLog(BaseModel):
    logs: str
    error: bool


class DockerBuildLog(BaseModel):
    build_log: DockerBuildLowLog
    push_log: Optional[DockerPushLog] = None
    error: bool


class DockerResources(BaseModel):
    mem_limit: Optional[int] = None
    mem_reservation: Optional[int] = None


class DockerVolume(BaseModel):
    orig: str
    dst: str
    mode: str = "rw"


class DockerRunResult(BaseModel):
    msg: str
    status: int


def docker_low_build(path, dockerfile, tag, rm=False) -> DockerBuildLowLog:
    """It uses the low API of python sdk.
    :param path: path to the Dockerfile
    :param dockerfile: name of the Dockerfile
    :param tag: fullname of the dokcer image to build
    :param rm: remove intermediate build images
    """

    # obj = _open_dockerfile(dockerfile)
    # build(fileobj=obj...
    _client = docker.APIClient(base_url="unix://var/run/docker.sock")
    generator = _client.build(path=path, dockerfile=dockerfile, tag=tag, rm=rm)
    error = False
    log_messages = ""
    while True:
        try:
            output = generator.__next__()
            output = output.decode().strip("\r\n")
            json_output = json.loads(output)
            if "stream" in json_output:
                logger.info(json_output["stream"].strip("\n"))
                log_messages += json_output["stream"]
            elif "errorDetail" in json_output:
                logger.error(json_output["error"])
                log_messages += json_output["error"]
                error = True

        except StopIteration:
            logger.info("Docker image build complete")
            log_messages += "Docker image build complete.\n"
            break
        except ValueError:
            logger.info("Error parsing output from docker image build: %s" % output)
            log_messages += "Error parsing output from docker image build:{output}\n"
            # raise ValueError(log)
            error = True

    return DockerBuildLowLog(error=error, logs=log_messages)


class DockerCommand:
    __slots__ = "docker"

    def __init__(self, docker_client=None):
        self.docker = docker_client or docker.from_env()

    def _wait_result(
        self, container: docker.models.containers.Container, timeout: int
    ) -> Union[Dict[str, Any], None]:
        result = None
        try:
            result = container.wait(timeout=timeout)
        except Exception:
            pass
        return result

    def _vol2dict(self, vol: DockerVolume) -> Dict[str, Any]:
        return {vol.orig: {"bind": vol.dst, "mode": vol.mode}}

    def run(
        self,
        cmd: str,
        image: str,
        *,
        timeout: int = 120,
        env_data: Dict[str, Any] = {},
        remove: bool = True,
        require_gpu: bool = False,
        gpu_count: int = -1,
        network_mode: str = "bridge",
        ports=None,
        resources=DockerResources(),
        volumes: List[DockerVolume] = [],
    ) -> DockerRunResult:
        # runtime = None
        device_requests = []
        if require_gpu:
            # runtime = "nvidia"
            device_requests = [
                docker.types.DeviceRequest(count=gpu_count, capabilities=[["gpu"]])
            ]

        logs = ""
        status_code = -1
        try:
            vols = [self._vol2dict(v) for v in volumes]
            logger.debug(f"image: {image}, cmd: {cmd}, gpu: {require_gpu}")
            container = self.docker.containers.run(
                image,
                cmd,
                # runtime=runtime,
                detach=True,
                environment=env_data,
                network_mode=network_mode,
                device_requests=device_requests,
                volumes=vols,
                ports=ports,
                **resources.dict(),
            )
            result = self._wait_result(container, timeout)
            if not result:
                container.kill()
            else:
                status_code = result["StatusCode"]
            logs = container.logs().decode("utf-8")
            if remove:
                container.remove()
        except docker.errors.ContainerError as e:
            logger.error(str(e))
            logs = str(e)
            status_code = -2
        except docker.errors.APIError as e:
            logs = str(e)
            logger.error(str(e))
            status_code = -3

        for line in logs:
            logger.debug(line)
        return DockerRunResult(msg=logs, status=status_code)

    def build(
        self, path: str, dockerfile: str, tag: str, version: str, rm=False, push=False
    ) -> DockerBuildLog:
        """Build docker
        :param path: path to the Dockerfile
        :param dockerfile: name of the Dockerfile
        :param tag: fullname of the dokcer image to build
        :param rm: remove intermediate build images
        :param push: Push docker image to a repository
        """

        error = False
        error_build = False
        error_push = False

        build_log = docker_low_build(path, dockerfile, tag, rm)
        if not build_log.error:
            img = self.docker.images.get(tag)
            img.tag(tag, tag=version)

        error_build = build_log.error

        push_log = None
        if push:
            # push_log = docker_push_image(tag)
            push_log = self.push_image(f"{tag}:{version}")
            error_push = push_log.error

        if error_build or error_push:
            error = True

        return DockerBuildLog(build_log=build_log, push_log=push_log, error=error)

    def push_image(self, tag) -> DockerPushLog:
        """
        Push to docker registry
        :param tag: full name of the docker image to push, it should include
        the registry url
        """

        error = False
        try:
            push_log_str = self.docker.images.push(tag)
        except docker.errors.APIError as e:
            error = True
            logger.error(str(e))
            push_log_str = str(e)

        return DockerPushLog(logs=push_log_str, error=error)

    def pull_image(self, repository, *, tag=None):
        """
        pull an image from a registry.
        If the full image is:
        http://localhost:5001/nuxion/python:3.8-slim
        then:
        repository="localhost:5001/nuxion/python
        tag="3.8-slim"
        :param repostory: simple image or repostory
        :param tag: tag version
        the registry url
        """

        self.docker.images.pull(repository, tag=tag)
