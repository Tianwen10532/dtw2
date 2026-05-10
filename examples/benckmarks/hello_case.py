

import dtw

@dtw.remote
class F1:
    def __init__(self,name):
        self.name=name
    def hello(self):
        return f'hello from {self.name}!'
    
    def commit_F2(self,url,rtime,name):
        @dtw.remote
        class F2:
            def __init__(self,name):
                self.name=name
            def hello(self):
                return f'hello from {self.name}!'
        f2=F2.res_req(target_cluster_url=url,runtime=rtime).task_cha().remote(name)
        return f2

    
f1 = F1.res_req(target_cluster_url="http://10.0.1.10:30080",runtime='pod').task_cha().remote("F1")
f2 = f1.commit_F2.remote("http://10.0.2.10:30080",'pod','F1')
print(f2)

f2 = dtw.get(f2)
print(f2)

f2ip=dtw.get(f2.hello.remote())
print(f2ip)

f1.free()
f2.free()