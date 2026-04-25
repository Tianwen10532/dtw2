
import textwrap


RUNTIME_IMAGE = "10.156.168.113:7887/pytorch/ray:py312-cu128-deps-v2"


def gen_rayjob_yaml(script: str, rayjob_name: str, gpu: int = 0) -> str:
    head_gpu_requests = ""
    head_gpu_limits = ""
    worker_gpu_requests = ""
    worker_gpu_limits = ""
    if gpu > 0:
        head_gpu_requests = f'\n                nvidia.com/gpu: "{gpu}"'
        head_gpu_limits = f'\n              limits:\n                nvidia.com/gpu: "{gpu}"'
        worker_gpu_requests = f'\n                nvidia.com/gpu: "{gpu}"'
        worker_gpu_limits = f'\n              limits:\n                nvidia.com/gpu: "{gpu}"'
    return f"""
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: {rayjob_name}

spec:
  backoffLimit: 0
  entrypoint: python actorserve.py #入口命令

  rayClusterSpec:
    rayVersion: "2.50.1"
    headGroupSpec:
      serviceType: NodePort
      rayStartParams:
        dashboard-host: "0.0.0.0"
      template:
        metadata: {{}}
        spec:
          containers:
          - image: {RUNTIME_IMAGE}
            name: ray-head
            command: ["/bin/bash", "-c", "--"]
            args:
              - |
                cd ~;

                read -r -d '' SCRIPT << EOM
{textwrap.indent(script,' '*16)}
                EOM
                printf "%s" "$SCRIPT" > "actorserve.py";
                
                ulimit -n 65536; echo head; $KUBERAY_GEN_RAY_START_CMD
            envFrom:
              - configMapRef: 
                  name: dtw-config
            env:
            - name: NODE_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.hostIP
            ports:
            - containerPort: 6379
              name: gcs
            - containerPort: 8265
              name: dashboard
            - containerPort: 10001
              name: client
            - containerPort: 8000
              name: serve
            - containerPort: 50051   # <-- gRPC 服务端口
              name: invoke
            - containerPort: 50052
              name: receiver
    workerGroupSpecs:
    - groupName: small-group
      maxReplicas: 5
      minReplicas: 1
      numOfHosts: 1
      rayStartParams: {{}}
      replicas: 1
      scaleStrategy: {{}}
      template:
        metadata: {{}}
        spec:
          containers:
          - image: {RUNTIME_IMAGE}
            name: ray-worker
            command: ["/bin/bash", "-c", "--"]
            args:
              - |
                ulimit -n 65536; echo worker; $KUBERAY_GEN_RAY_START_CMD
                tail -f /dev/null
            resources:
              requests:
                cpu: "1"{worker_gpu_requests}{worker_gpu_limits}
            envFrom:
              - configMapRef: 
                  name: dtw-config
            env:
            - name: NODE_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.hostIP    
  runtimeEnvYAML: |
    pip:
      # - grpcio==1.75.1 
      # - grpcio-tools==1.75.1
      - git+https://github.com/Tianwen10532/dtw2.git
  #   env_vars:
  #     counter_name: "test_counter"
  shutdownAfterJobFinishes: true
  submissionMode: K8sJobMode
  ttlSecondsAfterFinished: 10
  """


def gen_pod_yaml(script: str, pod_name: str, gpu: int = 0) -> str:
    gpu_resources = ""
    gpu_limits = ""
    if gpu > 0:
        # Keep same indentation level as "cpu" under resources.requests.
        gpu_resources = f'\n        nvidia.com/gpu: "{gpu}"'
        gpu_limits = f'\n      limits:\n        nvidia.com/gpu: "{gpu}"'

    return f"""
apiVersion: v1
kind: Pod
metadata:
  name: {pod_name}
  labels:
    app: {pod_name}
spec:
  restartPolicy: Never
  containers:
  - name: dtw-runtime
    image: {RUNTIME_IMAGE}
    command: ["/bin/bash", "-c", "--"]
    args:
      - |
        cd ~;

        read -r -d '' SCRIPT << EOM
{textwrap.indent(script, ' '*8)}
        EOM
        printf "%s" "$SCRIPT" > "actorserve.py";

        python -m pip install --no-cache-dir git+https://github.com/Tianwen10532/dtw2.git;
        ulimit -n 65536; python actorserve.py
    env:
    - name: NODE_IP
      valueFrom:
        fieldRef:
          fieldPath: status.hostIP
    envFrom:
      - configMapRef: 
          name: dtw-config
    ports:
    - containerPort: 50051
      name: invoke
    - containerPort: 50052
      name: receiver
    resources:
      requests:
        cpu: "1"{gpu_resources}
{gpu_limits}
---
apiVersion: v1
kind: Service
metadata:
  name: {pod_name}
spec:
  type: NodePort
  selector:
    app: {pod_name}
  ports:
  - name: invoke
    port: 50051
    targetPort: invoke
    protocol: TCP
  - name: receiver
    port: 50052
    targetPort: receiver
    protocol: TCP
"""
