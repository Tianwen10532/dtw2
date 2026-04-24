

from dtw.fed_object import DtwObject

import ray

_SENDER_PROXY_ACTOR_NAME = 'SenderProxyActor'
_RECEIVER_PROXY_ACTOR_NAME = 'ReceiverProxyActor'

def receiver_proxy_actor_name() -> str:
    global _RECEIVER_PROXY_ACTOR_NAME
    return _RECEIVER_PROXY_ACTOR_NAME

def recv(arg:DtwObject):
    receiver_proxy = ray.get_actor(receiver_proxy_actor_name)
    return receiver_proxy.get_data.remote(arg)

