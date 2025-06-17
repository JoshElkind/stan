import k8s_runner 
import os
print("KUBECONFIG:", os.getenv("KUBECONFIG"))
result = k8s_runner.run_algo_k8s(
    s3_key="upload533.py",
    func_name="upload533",
    table_name="1min_cBTC"
)
print("FINAL RESULT:", result)