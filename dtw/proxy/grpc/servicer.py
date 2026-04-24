
import grpc
import time
import json
import queue
import threading
import ray
from concurrent import futures
from dtw.grpc.invoke.invoke_pb2 import InvokeResponse, ResultResponse,StartActorResponse
from dtw.grpc.invoke import invoke_pb2_grpc
from dtw.grpc.invoke import invoke_pb2
import uuid
import datetime

from dtw.fed_object import DtwObject
# from dtw.proxy.barriers import recv
from dtw.proxy.grpc.grpc_proxy import GrpcReceiverProxy,send_result
import cloudpickle

# # 全局队列和结果表
# control_mailbox = queue.Queue()
# results = {}  # objref_id -> {"ref": ObjectRef, "ready": bool, "value": str}

class InvokerServicer(invoke_pb2_grpc.InvokerServicer):

    def __init__(self,worker_cls, my_env, receiver_proxy):
        ray.init(ignore_reinit_error=True)
        self.worker_cls=worker_cls
        self.my_env = my_env
        self.ray_actor_handle=None
        self.has_started=False
        self.actor_name=None
        self.results = {} #(uuid,srcparty)->RayObjectRef|value
        self.recv_proxy=receiver_proxy

        self.control_mailbox = queue.Queue()
        self.data_mailbox = []
        self.party_info={}


    def StartActor(self, request, context):
        party = request.your_global_id
        self.party_info = {
            "cmail": party.cmail,
            "cport": party.cport,
            "dmail": party.dmail,
            "dport": party.dport,
        }
        args, kwargs = ((), {})
        if request.args_pickle:
            args, kwargs = cloudpickle.loads(request.args_pickle)
        task = {
            "type": "start_actor",
            "args": args,
            "kwargs": kwargs,
        }
        self.control_mailbox.put(task)
        return StartActorResponse(success=True, error="NULL")

    def Invoke(self, request, context):
        args, kwargs = ((), {})
        if request.args_pickle:
            args, kwargs = cloudpickle.loads(request.args_pickle)

        uid = uuid.uuid4()
        task = {
            "type":"invoke",
            "method":request.method,
            "args": args,
            "kwargs": kwargs,
            "uid": uid,
        }
        srcipport = self.party_info['cmail']+":"+self.party_info['cport']
        self.control_mailbox.put(task)

        # results[objref_id] = {"ref": None, "ready": False, "value": ""}
        # print(f"[gRPC] Enqueued task {objref_id}")
        # return InvokeResponse(objref=objref_id)

        return InvokeResponse(success=True,Objid=str(uid),SrcIpPort=srcipport,error="")

    def GetResult(self, request, context):
        task = {
            "type":"resultget",
            "uid": request.Objid,
            "to_ip_port": request.ToIpPort,
        }
        self.data_mailbox.append(task)
        return ResultResponse(success=True)
    
    def GetData(self,request, context):
        uid = request.Objid

        if uid not in self.results.keys():
            return invoke_pb2.GetDataResponse(
                data = b"Not ready",
                ready=False
            )
        obj_ref = self.results[uid]
        ready, _ = ray.wait([obj_ref], timeout=0)
        if(not ready):
            return invoke_pb2.GetDataResponse(
                data = b"Not ready",
                ready=False
            )
        else:
            data_obj = ray.get(obj_ref)
            data_bytes = cloudpickle.dumps(data_obj)
            return invoke_pb2.GetDataResponse(
                data=data_bytes,
                ready=True
            )



def worker_loop(ivk_serv:InvokerServicer):
    """单线程执行队列中的任务"""
    while True:
        task = ivk_serv.control_mailbox.get()
        ttype=task["type"]
        
        print(f"worker_loop:{task}")

        if(ttype=="start_actor"):
            if(ivk_serv.has_started):
                continue
            resolved_args, resolved_kwargs = resolve_dependencies(task,ivk_serv)
            actor_handles = ivk_serv.worker_cls.options(runtime_env={"env_vars": ivk_serv.my_env}).remote(*resolved_args, **resolved_kwargs)
            ivk_serv.ray_actor_handle=actor_handles
            ivk_serv.has_started=True

        else:
            resolved_args, resolved_kwargs = resolve_dependencies(task,ivk_serv)
            # task = {
            #     "type":"invoke",
            #     "method":request.method,
            #     "args": args,
            #     "kwargs": kwargs,
            #     "uid": uid,
            # }
            actor=ivk_serv.ray_actor_handle
            result_ref = getattr(actor,task["method"]).remote(*resolved_args, **resolved_kwargs)
            ivk_serv.results[str(task['uid'])]=result_ref

            # print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            # print(ray.get(result_ref))

        # if not actor:
        #     print(f"[Worker] Unknown actor {task['actor']}")
        #     continue
        # method = getattr(actor, task["method"])
        # ref = method.remote(*task["args"])
        # results[task["id"]]["ref"] = ref
        # print(f"[Worker] Executing {task['method']} -> {task['id']}")
        # task_queue.task_done()

def poll_and_send_results(ivl_serv:InvokerServicer):
    sent_uids=set()
    while True:
        # send_tasks=[]
        for task in list(ivl_serv.data_mailbox):
            uid = task['uid']
            to_ip_port = task["to_ip_port"]

            # if uid in sent_uids:
            #     continue
            # 不存在结果
            if uid not in ivl_serv.results.keys():
                continue

            obj_ref = ivl_serv.results[uid]
            ready, _ = ray.wait([obj_ref], timeout=0)
            if ready:
                # print("SEND")
                data_obj = ray.get(obj_ref)
                data_bytes = cloudpickle.dumps(data_obj)
                send_result(to_ip_port,uid,data_bytes)
                sent_uids.add(uid)



def serve(actor_cls, my_env,addr="0.0.0.0:50051", rcv_addr="0.0.0.0:50052"):
    receiver_proxy=GrpcReceiverProxy(rcv_addr)
    receiver_proxy.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1),options=[('grpc.max_metadata_size', 4 * 1024 * 1024)])
    ivk_serv = InvokerServicer(actor_cls,my_env,receiver_proxy)
    invoke_pb2_grpc.add_InvokerServicer_to_server(ivk_serv, server)
    server.add_insecure_port(addr)

    # 启动 worker 线程（daemon）
    t = threading.Thread(target=worker_loop, args=(ivk_serv,),daemon=True)
    t.start()

    # 启动sender 线程
    s = threading.Thread(target=poll_and_send_results, args=(ivk_serv,),daemon=True)
    s.start()

    server.start()
    print(f"gRPC server started on {addr}")
    server.wait_for_termination()


def resolve_dependencies(task,ivk_serv:InvokerServicer):
    # TODO 展平参数
    args, kwargs = task['args'],task['kwargs']
    flattened_args = list(args)

    indexes = []
    resolved = []
    for idx, arg in enumerate(flattened_args):
        if isinstance(arg, DtwObject):
            indexes.append(idx)
            received_obj = recv(arg,ivk_serv)
            resolved.append(received_obj)

    if resolved:
        for idx, actual_val in zip(indexes, resolved):
            flattened_args[idx]=actual_val

    return tuple(flattened_args),{}


def recv(arg,ivk_serv:InvokerServicer):
    # TODO ResultRequest调用
    # arg
    # @dataclass
    # class DtwObject:
    #     uuid:str
    #     host:str
    #     port:int

    toipport=ivk_serv.party_info["dmail"]+":"+ivk_serv.party_info["dport"]

    channel = grpc.insecure_channel(arg.host+":"+str(arg.port))
    stub = invoke_pb2_grpc.InvokerStub(channel)
    req = invoke_pb2.ResultRequest(Objid=arg.uuid,ToIpPort=toipport)
    resp=stub.GetResult(req)
    print(resp)

    arg = ivk_serv.recv_proxy.get_data(arg.uuid)
    return arg
