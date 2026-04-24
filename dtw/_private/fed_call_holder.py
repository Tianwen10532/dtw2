

from dtw.fed_object import DtwObject
from .control_client import call_remote

from dtw.grpc.invoke import invoke_pb2,invoke_pb2_grpc
import grpc

class FedCallHolder:
    """
    `FedCallHolder` represents a call node holder when submitting tasks.
    For example,

    f.party("ALICE").remote()
    ~~~~~~~~~~~~~~~~
        ^
        |
    it's a holder.

    """
    def __init__(
        self,
        node_party,
        submit_ray_task_func,
        options={},
    ) -> None:
        # Note(NKcqx): FedCallHolder will only be created in driver process, where
        # the GlobalContext must has been initialized.
        # job_name = get_global_context().get_job_name()
        # self._party = fed_config.get_cluster_config(job_name).current_party
        self._node_party = node_party
        self._options = options
        self._submit_ray_task_func = submit_ray_task_func


class FedActorMethod:
    def __init__(
        self,
        method_name,
        fed_actor_handle,
    ):
        self.method_name=method_name
        self.fed_actor_handle=fed_actor_handle
        self._remote_actor_handle = self.fed_actor_handle._remote_actor_handle
        # {'status': 'success', 'cluster': '192.168.117.4', 'ivk_port': 31748, 'recv_port': 30319, 'rayjob_name': 'dtwrj-68iye3'}
    
    def remote(self, *args, **kwargs) -> DtwObject:
        url = self._remote_actor_handle['cluster']+':'+str(self._remote_actor_handle['ivk_port'])
        channel = grpc.insecure_channel(url)
        stub = invoke_pb2_grpc.InvokerStub(channel)
        resp = call_remote(stub, self.method_name, *args, **kwargs)
        # Objid: "81d5a37e-6a3c-42de-abd6-524c80ffe324"
        # SrcIpPort: "192.168.117.4:31234"
        
        return DtwObject(uuid=resp.Objid, host=resp.SrcIpPort.split(':')[0],port=int(resp.SrcIpPort.split(':')[1]))