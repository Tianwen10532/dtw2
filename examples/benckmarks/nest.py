

import dtw

@dtw.remote
class F1:
    
    def get_node_ip(self):
        import os
        self.node_name=os.environ.get("NODE_IP",None)
        return os.environ.get("NODE_IP")
    
    def commit_F2(self,url,rtime):
        @dtw.remote
        class F2:
            def get_node_ip(self):
                import os
                self.node_name=os.environ.get("NODE_IP",None)
                return os.environ.get("NODE_IP")
        f2=F2.res_req(target_cluster_url=url,runtime=rtime).task_cha().remote()
        return f2

    
f1 = F1.res_req(target_cluster_url="http://10.0.1.10:30080",runtime='pod').task_cha().remote()

f1ip = dtw.get(f1.get_node_ip.remote())
print(f1ip)

f2 = f1.commit_F2.remote("http://10.0.2.10:30080",'pod')
print(f2)

f2 = dtw.get(f2)


f2ip=dtw.get(f2.get_node_ip.remote())
print(f2ip)

f1.free()
f2.free()