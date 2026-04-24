
import dtw

@dtw.remote
class F1:
    def get_node_ip(self):
        import os
        self.node_name=os.environ.get("NODE_IP",None)
        return os.environ.get("NODE_IP")
    
    def commit_F1(self,url,rtime):
        @dtw.remote
        class F2:
            def get_node_ip(self):
                import os
                self.node_name=os.environ.get("NODE_IP",None)
                return os.environ.get("NODE_IP")
        return F2._source_cache
    
    def free_F1(self):
        self.actor_handle.free()
        return "YES"
    
f1 = F1.res_req().task_cha().remote("123",'345')