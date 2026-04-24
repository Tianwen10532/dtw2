
import grpc
import time
from dtw.grpc.fed import fed_pb2_grpc, fed_pb2
import threading
from concurrent import futures
import cloudpickle

class SendDataService(fed_pb2_grpc.GrpcServiceServicer):
    def __init__(self,all_data):
        self._all_data = all_data

    def SendData(self, request, context):
        # print(f"收到数据: Objid={request.Objid}, len={len(request.data)}")
        self._all_data[request.Objid]=cloudpickle.loads(request.data)
        return fed_pb2.SendDataResponse(code=200, result="OK")

class GrpcReceiverProxy():
    def __init__(
        self,
        listen_addr:str="0.0.0.0:50052"
    ) -> None:
        self.listen_addr = listen_addr
        self.all_data = {}  #map from (uuid) to data

    def _serve(self):
        # port = self.listen_addr[self._listen_addr.index(':') + 1 :]
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        fed_pb2_grpc.add_GrpcServiceServicer_to_server(SendDataService(self.all_data), server)
        server.add_insecure_port(self.listen_addr)
        server.start()
        print("start")
        server.wait_for_termination()

    def start(self):
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        time.sleep(1)

    def get_data(self, uid):
        while True:
            if uid in self.all_data:
                return self.all_data[uid]
            time.sleep(0.5)

def send_result(to_ip_port: str, uid: str, data: bytes):
    channel = grpc.insecure_channel(to_ip_port)
    stub = fed_pb2_grpc.GrpcServiceStub(channel)
    req = fed_pb2.SendDataRequest(
        data=data,   # bytes 类型
        Objid=uid    # string 类型
    )
    resp = stub.SendData(req)
    return resp
    
if __name__ == "__main__":
    grp = GrpcReceiverProxy()
    grp.start()
    
    print(grp.get_data("1234_objid"))