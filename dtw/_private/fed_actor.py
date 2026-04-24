
import inspect
import json
import os
import re
import socket
import time
from typing import Any
from urllib.parse import urlparse
import grpc
import ray
import requests

from dtw.utils import random_suffix
from dtw.log.logger import logger
import dtw.grpc.invoke.invoke_pb2 as invoke_pb2
import dtw.grpc.invoke.invoke_pb2_grpc as invoke_pb2_grpc
from dtw._private.fed_call_holder import FedActorMethod
from .control_client import start_actor


from .templates.rayjob import gen_pod_yaml, gen_rayjob_yaml



class FedActorHandle:
    def __init__(
        self,
        cls_name,
        party,
        options,
        res_req=None,
        task_cha=None,
        source_cache: str | None = None,
    ) -> None:
        self._cls_name=cls_name
        self._party = party
        self._options = options
        self._res_req = dict(res_req or {})
        self._task_cha = dict(task_cha or {})
        self._source_cache = source_cache
        self._ray_actor_handle = None
        self._remote_actor_handle = None

    def _get_stub(self):
        channel= grpc.insecure_channel(
            f"{self._remote_actor_handle['cluster']}:{self._remote_actor_handle['ivk_port']}"
        )
        return invoke_pb2_grpc.InvokerStub(channel)

    def _execute_impl(self, *cls_args, **cls_kwargs):
        if self._party == "local":
            self._ray_actor_handle = (
                ray.remote(self._body)
                .options(**self._options)
                .remote(*cls_args, **cls_kwargs)
            )
            return

        self._remote_actor_handle = generate_rayjob_yaml(
            res_req=self._res_req,
            task_cha=self._task_cha,
            source_cache=self._source_cache,
            cls_name=self._cls_name
        )

        stub=self._get_stub()
        your_global_id = invoke_pb2.PartyId(
            cmail=self._remote_actor_handle["cluster"],
            cport=str(self._remote_actor_handle["ivk_port"]),
            dmail=self._remote_actor_handle["cluster"],
            dport=str(self._remote_actor_handle["recv_port"]),
        )
        start_actor(stub, your_global_id, *cls_args, **cls_kwargs)

    def __getattr__(self, method_name: str):
        # getattr(self._body, method_name)
        return FedActorMethod(method_name, self)

    def free(self, route_url: str | None = None) -> dict[str, Any]:
        if self._remote_actor_handle:
            used_route_url = route_url or self._remote_actor_handle.get("route_url")
            normalized_route = _normalize_route_url(used_route_url)
            url = normalized_route + "delete_rayjobname/"
            namespace = self._remote_actor_handle.get("namespace", _default_namespace())
            runtime = _normalize_runtime(self._remote_actor_handle.get("runtime", "rayjob"))
            resource_name = (
                self._remote_actor_handle.get("resource_name")
                or self._remote_actor_handle.get("rayjob_name")
            )
            if not resource_name:
                return {
                    "status": "error",
                    "message": "missing resource_name for delete",
                }
            cluster_base_url = self._remote_actor_handle.get("cluster_base_url")
            if not cluster_base_url and _is_scheduler_endpoint(normalized_route):
                cluster_base_url = _resolve_cluster_base_url(
                    normalized_route, self._remote_actor_handle.get("cluster")
                )
                if cluster_base_url:
                    self._remote_actor_handle["cluster_base_url"] = cluster_base_url

            scheduler_payload = {
                "runtime": runtime,
                "resource_name": resource_name,
                "rayjob_name": resource_name,
                "namespace": namespace,
                "cluster_base_url": cluster_base_url,
            }
            scheduler_payload = {
                k: v for k, v in scheduler_payload.items() if v is not None
            }

            try:
                if _is_scheduler_endpoint(normalized_route) and "cluster_base_url" not in scheduler_payload:
                    raise RuntimeError(
                        "cluster_base_url is required for scheduler delete but could not be resolved"
                    )
                response = requests.post(url, json=scheduler_payload, timeout=20)
                response.raise_for_status()
                return response.json()
            except Exception as first_exc:
                # Compatibility fallback for legacy delete payload.
                legacy_payload = {
                    "cluster_ip": self._remote_actor_handle.get("cluster"),
                    "rayjob_name": resource_name,
                    "namespace": namespace,
                }
                legacy_payload = {
                    k: v for k, v in legacy_payload.items() if v is not None
                }
                try:
                    response = requests.post(url, json=legacy_payload, timeout=20)
                    response.raise_for_status()
                    return response.json()
                except Exception as second_exc:
                    logger.exception(
                        "free failed resource=%s route_url=%s",
                        resource_name,
                        normalized_route,
                    )
                    return {
                        "status": "error",
                        "message": "delete runtime resource failed",
                        "resource_name": resource_name,
                        "runtime": runtime,
                        "scheduler_error": str(first_exc),
                        "legacy_error": str(second_exc),
                    }

        if self._ray_actor_handle is not None:
            try:
                ray.kill(self._ray_actor_handle, no_restart=True)
                return {"status": "success", "message": "local actor killed"}
            except Exception as exc:
                return {
                    "status": "error",
                    "message": "failed to kill local actor",
                    "error": str(exc),
                }

        return {"status": "noop", "message": "actor already released"}


def _normalize_runtime(raw_runtime: Any) -> str:
    runtime = str(raw_runtime or "rayjob").strip().lower()
    if runtime not in {"rayjob", "pod"}:
        raise ValueError(f"res_req.runtime must be 'rayjob' or 'pod', got: {raw_runtime}")
    return runtime

def _normalize_gpu_count(raw_gpu: Any) -> int:
    try:
        gpu = int(raw_gpu)
    except Exception as exc:
        raise ValueError(f"res_req.gpu must be an integer, got: {raw_gpu}") from exc
    if gpu < 0:
        raise ValueError(f"res_req.gpu must be >= 0, got: {raw_gpu}")
    return gpu

def _normalize_route_url(route_url: str | None) -> str:
    route = route_url or os.getenv("DTW_ROUTE_URL", "http://10.156.169.81:8001/")
    return route.rstrip("/") + "/"

def _default_namespace() -> str:
    return os.getenv("DTW_NAMESPACE", "default")

def _get_node_ip() -> str:
    return os.getenv("NODE_IP", socket.gethostname())

def _default_required_labels() -> dict[str, Any]:
    raw = os.getenv("DTW_REQUIRED_LABELS", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("DTW_REQUIRED_LABELS must be a JSON object string.") from exc
    if not isinstance(parsed, dict):
        raise ValueError("DTW_REQUIRED_LABELS must decode to an object.")
    return parsed

def _default_task_character() -> dict[str, Any]:
    return {
        "priority": os.getenv("DTW_TASK_PRIORITY", "normal"),
        "tenant": os.getenv("DTW_TASK_TENANT", "default"),
    }

def _normalize_apply_response(body: dict[str, Any]) -> dict[str, Any]:
    # scheduler response: {"cluster_apply_response": {...}}
    if "cluster_apply_response" in body:
        cluster_resp = body.get("cluster_apply_response") or {}
        if not isinstance(cluster_resp, dict):
            raise RuntimeError(f"Unexpected scheduler response: {body}")
        merged = dict(cluster_resp)
        selected_cluster = body.get("selected_cluster_base_url")
        if selected_cluster and "cluster_base_url" not in merged:
            merged["cluster_base_url"] = selected_cluster
        return merged

    # direct dtw-cluster response
    return body

def _is_scheduler_endpoint(route_url: str) -> bool:
    # dtw-scheduler exposes /clusters; dtw-cluster does not.
    try:
        resp = requests.get(route_url + "clusters", timeout=5)
        return resp.ok
    except Exception:
        return False
    
def _resolve_cluster_base_url(route_url: str, cluster_ip: str | None) -> str | None:
    if not cluster_ip:
        return None
    try:
        resp = requests.get(route_url + "clusters", timeout=5)
        resp.raise_for_status()
        data = resp.json()
        clusters = data.get("clusters", []) if isinstance(data, dict) else []
        for item in clusters:
            base_url = item.get("base_url")
            if not base_url:
                continue
            host = urlparse(base_url).hostname
            if host == cluster_ip:
                return base_url
    except Exception:
        return None
    return None

def create_actor_req(
    resource_yaml: str,
    route_url: str | None = None,
    res_req: dict[str, Any] | None = None,
    task_cha: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_route = _normalize_route_url(route_url)
    url = normalized_route + "apply"
    timeout_seconds = int(os.getenv("DTW_APPLY_TIMEOUT_SECONDS", "300"))
    namespace = _default_namespace()

    resource_request = {
        "source_node":_get_node_ip(),
        "scheduler_policy": os.getenv("DTW_SCHEDULER_POLICY", "round_robin"),
        "required_labels": _default_required_labels(),
        "dry_run": False,
    }
    if res_req:
        resource_request.update(res_req)
    resource_request["runtime"] = _normalize_runtime(resource_request.get("runtime", "rayjob"))
    if (
        resource_request.get("cluster_base_url") is None
        and resource_request.get("target_cluster_url") is not None
    ):
        resource_request["cluster_base_url"] = resource_request.get("target_cluster_url")
    if "gpu" in resource_request:
        gpu = _normalize_gpu_count(resource_request.get("gpu"))
        resource_request["gpu"] = gpu
        required_labels = resource_request.get("required_labels")
        if required_labels is None:
            required_labels = {}
            resource_request["required_labels"] = required_labels
        if not isinstance(required_labels, dict):
            raise ValueError("res_req.required_labels must be a dict.")
        required_labels["gpu"] = gpu

    task_character = _default_task_character()
    if task_cha:
        task_character.update(task_cha)

    scheduler_payload = {
        "dtw-content": {
            "resource_yaml": resource_yaml,
            "namespace": namespace,
            "timeout_seconds": timeout_seconds,
        },
        "dtw-resource-request": resource_request,
        "dtw-task-character": task_character,
    }
    cluster_base_url = os.getenv("DTW_CLUSTER_BASE_URL")
    if cluster_base_url:
        scheduler_payload["dtw-resource-request"]["cluster_base_url"] = cluster_base_url

    #发送请求
    response = requests.post(url, json=scheduler_payload, timeout=timeout_seconds + 15)
    if not response.ok and response.status_code in {404, 405, 415, 422}:
        # Compatibility path for direct dtw-cluster /apply.
        logger.warning(
            "scheduler apply format rejected(status=%s), fallback to cluster apply format",
            response.status_code,
        )
        legacy_payload = {
            "resource_yaml": resource_yaml,
            "namespace": namespace,
            "timeout_seconds": timeout_seconds,
        }
        response = requests.post(url, json=legacy_payload, timeout=timeout_seconds + 15)

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        body = response.text
        raise RuntimeError(
            f"Apply request failed: status={response.status_code}, url={url}, body={body}"
        ) from exc
    body = response.json()
    normalized = _normalize_apply_response(body)

    required = ("cluster", "ivk_port", "recv_port")
    missing = [k for k in required if normalized.get(k) in (None, "")]
    if missing:
        raise RuntimeError(f"Apply response missing keys: {missing}, body={body}")
    if normalized.get("resource_name") in (None, "") and normalized.get("rayjob_name") not in (None, ""):
        normalized["resource_name"] = normalized.get("rayjob_name")
    if normalized.get("rayjob_name") in (None, "") and normalized.get("resource_name") not in (None, ""):
        normalized["rayjob_name"] = normalized.get("resource_name")
    if normalized.get("resource_name") in (None, ""):
        raise RuntimeError(f"Apply response missing resource_name/rayjob_name, body={body}")

    if not normalized.get("cluster_base_url") and _is_scheduler_endpoint(normalized_route):
        inferred = _resolve_cluster_base_url(normalized_route, normalized.get("cluster"))
        if inferred:
            normalized["cluster_base_url"] = inferred

    normalized["route_url"] = normalized_route
    return normalized


def generate_rayjob_yaml(
    res_req: dict[str, Any] | None = None,
    task_cha: dict[str, Any] | None = None,
    source_cache: str | None = None,
    cls_name:str|None=None
) -> dict[str, Any]:
    runtime = _normalize_runtime((res_req or {}).get("runtime", "rayjob"))
    resource_name = f"dtw{runtime[:1]}-{random_suffix()}"
    requested_gpu = 0

    if res_req and "gpu" in res_req:
        requested_gpu = _normalize_gpu_count(res_req.get("gpu"))
    actor_remote_decorator = (
        f"@ray.remote(num_gpus={requested_gpu})" if requested_gpu > 0 else "@ray.remote"
    )

    dtw_wrapped_source = source_cache
    assert dtw_wrapped_source is not None,"Could not resolve actor class source. Please define the class in a regular .py file and decorate it with @dtw.remote."
    
    runtime_source_lines = dtw_wrapped_source.splitlines()
    while runtime_source_lines and runtime_source_lines[0].lstrip().startswith("@"):
        runtime_source_lines.pop(0)
    runtime_source = "\n".join(runtime_source_lines)
    runtime_cls_name = f"{cls_name}Runtime"
    runtime_source = re.sub(
        rf"^(\s*class\s+){re.escape(cls_name)}(\b)",
        rf"\1{runtime_cls_name}\2",
        runtime_source,
        count=1,
        flags=re.MULTILINE,
    )

    python_script = f"""import ray
import dtw
from dtw.proxy.grpc.servicer import serve
from dtw.grpc.invoke import invoke_pb2_grpc as invoke_pb2_grpc
import time
# Keep original @dtw.remote source for compatibility/debug.
{dtw_wrapped_source}

{actor_remote_decorator}
{runtime_source}
my_env = {{"SOURCE_CACHE" : {cls_name}._source_cache}}
actor_cls={runtime_cls_name}
serve(actor_cls,my_env,addr=\"0.0.0.0:50051\", rcv_addr=\"0.0.0.0:50052\")
"""
    
    if runtime == "pod":
        runtime_yaml = gen_pod_yaml(python_script, resource_name, gpu=requested_gpu)
    else:
        runtime_yaml = gen_rayjob_yaml(python_script, resource_name, gpu=requested_gpu)
    #提交
    response = create_actor_req(
        runtime_yaml,
        res_req=res_req,
        task_cha=task_cha,
    )
    response["runtime"] = runtime
    response["resource_name"] = response.get("resource_name") or response.get("rayjob_name") or resource_name
    if not response.get("rayjob_name"):
        response["rayjob_name"] = response["resource_name"]
    wait_for_port(response["cluster"], response["ivk_port"], response["recv_port"])
    return response


def wait_for_port(host: str, port1: int, port2: int, interval: float = 2.0):
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        try:
            sock.connect((host, port1))
            sock.close()
            break
        except (socket.timeout, ConnectionRefusedError, OSError):
            time.sleep(interval)

    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        try:
            sock.connect((host, port2))
            sock.close()
            return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            time.sleep(interval)