
from pydantic import BaseModel, Field, field_validator
from typing import Literal
import requests

class ApplyRequest(BaseModel):
    src_ip: str
    src_port: list[int] | None = None
    dst_ip: str | None = None
    dst_port: list[int] | None = None
    srv6_segs: list[str] = Field(min_length=1)
    protocol: Literal["tcp", "udp"] = "tcp"

    class Config:
        extra = "forbid"

    @field_validator("src_port", "dst_port", mode="before")
    @classmethod
    def normalize_ports(cls, v):
        if v is None:
            return v
        if isinstance(v, int):
            ports = [v]
        elif isinstance(v, list):
            ports = v
        else:
            raise ValueError("port must be int, list[int], or null")
        if len(ports) == 0:
            raise ValueError("port list must not be empty")
        normalized = sorted(set(int(p) for p in ports))
        for p in normalized:
            if p < 1 or p > 65535:
                raise ValueError("port must be in [1, 65535]")
        return normalized

class DeleteRequest(BaseModel):
    policy_id: str | None = None
    src_ip: str | None = None
    src_port: list[int] | None = None
    dst_ip: str | None = None
    dst_port: list[int] | None = None
    protocol: Literal["tcp", "udp"] = "tcp"

def network_route_apply(route,gateway,dst_ips,src_ip):
    data = ApplyRequest(
        src_ip=src_ip,
        src_port=None,
        dst_ip=dst_ips,
        dst_port=None,
        srv6_segs=route,
        protocol="tcp"
    )
    url = gateway+"/apply"
    response = requests.post(url, json=data.model_dump()).json()
    return data, response.get('policy_id')

def network_route_del(route,gateway,dst_ips,src_ip,policy_id):
    req_data = DeleteRequest(
        policy_id=policy_id,
        src_ip=src_ip,
        src_port=None,
        dst_ip=dst_ips,
        dst_port=None,
        protocol="tcp"
    )
    url = gateway+"/delete"
    response = requests.post(url, json=req_data.model_dump())
    return response
