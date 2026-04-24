import dtw
import ray

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

@ray.remote
class F1Runtime:
    def get_node_ip(self):
        import os
        self.node_name=os.environ.get("NODE_IP",None)
        return os.environ.get("NODE_IP")
    
    def get_source_cache(self):
        import os
        return os.environ.get("SOURCE_CACHE")
    
    def commit_F1(self,url,rtime):
        @dtw.remote
        class F2:
            def get_node_ip(self):
                import os
                self.node_name=os.environ.get("NODE_IP",None)
                return os.environ.get("NODE_IP")
        a = F2.remote()
        return F2._source_cache
    
    def free_F1(self):
        self.actor_handle.free()
        return "YES"
    
my_env = {"SOURCE_CACHE" : F1._source_cache}


f1 = F1Runtime.options(runtime_env={"env_vars": my_env}).remote()

a = f1.commit_F1.remote("123",'456')
ray.get(a)