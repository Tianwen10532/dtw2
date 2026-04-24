
import grpc
import cloudpickle

import dtw.grpc.invoke.invoke_pb2_grpc as invoke_pb2_grpc
import dtw.grpc.invoke.invoke_pb2 as invoke_pb2


def start_actor(stub, your_global_id, *args, **kwargs):
    payload = cloudpickle.dumps((args, kwargs))
    req = invoke_pb2.StartActorRequest(your_global_id=your_global_id, args_pickle=payload)
    resp = stub.StartActor(req)
    if not resp.success:
        raise RuntimeError(f"StartActor failed: {resp.error}")
    print(f"✅ Started actor")


def call_remote(stub, method, *args, **kwargs):
    payload = cloudpickle.dumps((args, kwargs))
    req = invoke_pb2.InvokeRequest(
        method=method,
        args_pickle=payload,
    )
    resp = stub.Invoke(req)
    if not resp.success:
        raise RuntimeError(resp.error)
    return resp


def main():
    channel = grpc.insecure_channel("192.168.117.52:31589")
    stub = invoke_pb2_grpc.InvokerStub(channel)

    # 启动 actor，带初始化参数
    start_actor(stub, "addworker", 101)

    # 调用 echo
    res = call_remote(stub, "addworker", "add", 10)
    print("echo ->", res)

    # 调用 add
    res = call_remote(stub, "addworker", "add", 2)
    print("add ->", res)

    # 多返回
    res = call_remote(stub, "addworker", "double", 2)
    print("pair ->", res)


if __name__ == "__main__":
    main()
