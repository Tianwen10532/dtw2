
import functools
import inspect
from typing import Any
import time
import cloudpickle
import grpc
import ast
import textwrap

from dtw._private.fed_actor import FedActorHandle
from dtw.fed_object import DtwObject
from dtw.grpc.invoke import invoke_pb2,invoke_pb2_grpc

#存储源代码的属性名
_DTW_CACHED_SOURCE_ATTR = "__dtw_cached_source__"

def remote(*args, **kwargs):
    def _make_fed_remote(function_or_class, **options):

        if inspect.isclass(function_or_class):
            return FedRemoteClass(function_or_class).options(**options)

        raise TypeError(
            "The @dtw.remote decorator must be applied to a class."
        )
    
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @fed.remote.
        return _make_fed_remote(args[0])
    assert len(args) == 0 and len(kwargs) > 0, "Remote args error."
    return functools.partial(_make_fed_remote, **kwargs)


class FedRemoteClass:
    def __init__(self, func_or_class) -> None:
        class_resouce = self._get_class_source(func_or_class)
        self._cls_name = func_or_class.__name__
        self._options = {}
        self._party = "dtwroute"
        self._res_req: dict[str, Any] = {}
        self._task_cha: dict[str, Any] = {}
        self._source_cache = class_resouce

    def _get_class_source(self,cls):
        try:
            return textwrap.dedent(inspect.getsource(cls))
        except OSError:
            qualname = cls.__qualname__  # 例如 "F1.commit_F1.<locals>.F2"
            parts = qualname.split(".<locals>.")
            class_name = cls.__name__
            if len(parts) == 2:
                outer_func_name = parts[0].split(".")[-1]  # 方法名 commit_F1
                outer_cls_name = parts[0].split(".")[0]

                import os
                SOURCE_CACHE=os.environ.get("SOURCE_CACHE")
                tree = ast.parse(SOURCE_CACHE)

                for node in tree.body:
                    if isinstance(node, ast.ClassDef) and node.name+"Runtime" == outer_cls_name:
                        for func_node in node.body:
                            if isinstance(func_node, ast.FunctionDef) and func_node.name == outer_func_name:
                                for inner_node in func_node.body:
                                    if isinstance(inner_node, ast.ClassDef) and inner_node.name == class_name:
                                        # 提取源码行
                                        lines = SOURCE_CACHE.splitlines()
                                        class_src = "\n".join(lines[inner_node.lineno - 1: inner_node.end_lineno])
                                        # print(class_src)
                                        return "@dtw.remote\n"+textwrap.dedent(class_src)
                raise OSError(f"在方法 {outer_func_name} 中找不到类 {class_name}")
            else:
                # 顶层类
                tree = ast.parse(SOURCE_CACHE)
                for node in tree.body:
                    if isinstance(node, ast.ClassDef) and node.name == class_name:
                        lines = SOURCE_CACHE.splitlines()
                        class_src = "\n".join(lines[node.lineno - 1: node.end_lineno])
                        return textwrap.dedent(class_src)
                raise OSError(f"无法在SOURCE_CACHE中找到类 {class_name}")

    @staticmethod
    def _normalize_kv(*args, **kwargs) -> dict[str, Any]:
        if len(args) > 1:
            raise TypeError("Only one positional dict is supported.")
        data: dict[str, Any] = {}
        if len(args) == 1:
            if not isinstance(args[0], dict):
                raise TypeError("Positional argument must be a dict.")
            data.update(args[0])
        data.update(kwargs)
        return data
    
    def options(self, **options):
        self._options = options
        return self
    
    def res_req(self, *args, **kwargs):
        self._res_req = self._normalize_kv(*args, **kwargs)
        if (
            "cluster_base_url" not in self._res_req
            and "target_cluster_url" in self._res_req
        ):
            self._res_req["cluster_base_url"] = self._res_req["target_cluster_url"]
        # self._node_party = "dtwroute"
        return self

    def task_cha(self, *args, **kwargs):
        self._task_cha = self._normalize_kv(*args, **kwargs)
        return self
    
    def party(self, party: str):
        self._party = party
        return self
    
    def remote(self, *cls_args, **cls_kwargs):
        # fed_class_task_id = get_global_context().next_seq_id()
        # job_name = get_global_context().get_job_name()

        fed_actor_handle = FedActorHandle(
            self._cls_name,
            self._party,
            self._options,
            self._res_req,
            self._task_cha,
            source_cache=self._source_cache,
        )
        
        # 初始化actor，注入参数
        fed_actor_handle._execute_impl(*cls_args, **cls_kwargs)
        return fed_actor_handle



def get(dtw_object:DtwObject):
    url = dtw_object.host+":"+str(dtw_object.port)
    channel = grpc.insecure_channel(url)
    stub = invoke_pb2_grpc.InvokerStub(channel)

    while True:
        req = invoke_pb2.GetDataRequest(Objid=str(dtw_object.uuid))
        res = stub.GetData(req)
        if(not res.ready):
            time.sleep(0.5)
            continue
        else:
            return cloudpickle.loads(res.data)