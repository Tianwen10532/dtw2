
import dtw


@dtw.remote
class F1:
    def mul2(self,num):
        print(f"F1 mul2({num})")
        return num*2
    
@dtw.remote
class F2:
    def add1(self,num):
        print(f"F2 add1({num})")
        return num+1
    
@dtw.remote
class F3:
    def __init__(self):
        self.value=1

    def mulval(self,num):
        print(f"F3 mulval({self.value},{num})")
        rel=num*self.value
        self.value=num
        return rel
    
@dtw.remote
class F4:
    def add(self,a,b):
        print(f"F3 mulval({a},{b})")
        return a+b
    

def main():
    f1 = F1.res_req(target_cluster_url="http://10.0.1.10:30080",runtime='pod').task_cha().remote()
    f2 = F2.res_req(target_cluster_url="http://10.0.2.10:30080",runtime='pod').task_cha().remote()
    f3 = F3.res_req(target_cluster_url="http://10.0.3.10:30080",runtime='pod').task_cha().remote()
    f4 = F4.res_req(target_cluster_url="http://10.0.4.10:30080",runtime='pod').task_cha().remote()


    xns=[1,2,3,4]
    for i in xns:
        print(f"i:{i}")
        f1o = f1.mul2.remote(i)
        print(f"f1o:{f1o}")
        f2o = f2.add1.remote(f1o)
        print(f"f2o:{f2o}")
        f3o = f3.mulval.remote(f1o)
        print(f"f3o:{f3o}")
        f4o = f4.add.remote(f2o,f3o)
        print(f"f4o:{f4o}")
        print(dtw.get(f4o))

    f1.free()
    f2.free()
    f3.free()
    f4.free()

if __name__ == "__main__":
    main()