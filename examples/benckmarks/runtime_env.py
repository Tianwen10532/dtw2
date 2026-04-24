import ray

ray.init()

@ray.remote
class MyActor:
    def get_env(self):
        import os
        return os.environ.get("MY_VAR")

# driver 设置环境变量
my_env = {"MY_VAR": "hello_world"}

# 创建 actor 时传递 runtime_env
actor = MyActor.options(runtime_env={"env_vars": my_env}).remote()

print(ray.get(actor.get_env.remote()))  # 输出: hello_world