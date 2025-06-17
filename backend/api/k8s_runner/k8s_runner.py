import uuid
import boto3
import os
import yaml
import time
import json
from dotenv import load_dotenv
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

load_dotenv()

def display_job_yaml_structure(job_yaml):
    print("🔍 Debugging job YAML structure...")
    print(f"Job name: {job_yaml.get('metadata', {}).get('name')}")
    containers = job_yaml.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    if containers:
        print(f"Container image: {containers[0].get('image')}")
        print(f"Environment Variables: {[env.get('name') + '=' + env.get('value', '') for env in containers[0].get('env', [])]}")
    else:
        print("⚠️ No containers defined in job YAML")

def run_algo_k8s(s3_key, func_name, table_name):
    job_id = f"algo-job-{uuid.uuid4().hex[:8]}"
    result_key = f"results/{job_id}.json"

    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        template_path = os.path.join(current_dir, "template.yaml")
        
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Template file not found at {template_path}")
        
        with open(template_path) as f:
            job_yaml = yaml.safe_load(f)

        job_yaml["metadata"]["name"] = job_id
        container = job_yaml["spec"]["template"]["spec"]["containers"][0]
        container["name"] = "algo-runner"
        container["image"] = "944816881019.dkr.ecr.us-east-1.amazonaws.com/algo-runner@sha256:aba6331c4d7ec7701cfd43c3f06744dd51873d2b5bd0b2cee66df64a4d901cf4"

        env_dict = {env["name"]: env for env in container.get("env", [])}
        env_dict.update({
            "S3_KEY": {"name": "S3_KEY", "value": s3_key},
            "FUNC_NAME": {"name": "FUNC_NAME", "value": func_name},
            "TABLE_NAME": {"name": "TABLE_NAME", "value": table_name},
            "RESULT_KEY": {"name": "RESULT_KEY", "value": result_key},
            "AWS_STORAGE_BUCKET_NAME": {"name": "AWS_STORAGE_BUCKET_NAME", "value": os.getenv("AWS_STORAGE_BUCKET_NAME")},
            "AWS_REGION": {"name": "AWS_REGION", "value": os.getenv("AWS_REGION")}
        })
        container["env"] = list(env_dict.values())

        try:
            config.load_kube_config(config_file=os.path.expanduser("~/.kube/config"))
        except Exception as e:
            raise

        try:
            batch_v1 = client.BatchV1Api()
            core_v1 = client.CoreV1Api()
            auth_v1 = client.AuthorizationV1Api()

            core_v1.list_namespace(limit=1)

            access_review = client.V1SelfSubjectAccessReview(
                spec=client.V1SelfSubjectAccessReviewSpec(
                    resource_attributes=client.V1ResourceAttributes(
                        namespace="default", verb="create", group="batch", version="v1", resource="jobs"
                    )
                )
            )
            result = auth_v1.create_self_subject_access_review(body=access_review)
            if not result.status.allowed:
                raise PermissionError("insufficient permissions to create jobs in default namespace")

        except ApiException as e:
            if e.status == 401:
                print("auth failed")
                
            elif e.status == 403:
                print("auth failed")
            else:
                print("api failed")
            raise

        display_job_yaml_structure(job_yaml)

        print(yaml.dump(job_yaml))

        try:
            batch_v1.create_namespaced_job(body=job_yaml, namespace="default")
            
        except ApiException as e:
            print(f"failed to create job: {e}")
            if e.status == 422:
                print("error?")
            raise

        # monitoring pod
        pod_name = None
        job_failed = False
        
        for i in range(120):
            try:
                job = batch_v1.read_namespaced_job_status(name=job_id, namespace="default")
                status = job.status
                
              
                if not pod_name:
                    try:
                        pods = core_v1.list_namespaced_pod(namespace="default", label_selector=f"job-name={job_id}")
                        if pods.items:
                            pod_name = pods.items[0].metadata.name
                            
                    except Exception as e:
                        pass
                
                if status.succeeded == 1:
                   
                    # final logs even on success
                    if pod_name:
                        try:
                            logs = core_v1.read_namespaced_pod_log(name=pod_name, namespace="default")
                        
                        except Exception as log_error:
                            print("final logs!")
                    break
                elif status.failed:
                    job_failed = True
                    if pod_name:
                        try:
                            logs = core_v1.read_namespaced_pod_log(name=pod_name, namespace="default")
                           
                        except Exception as log_error:
                            print(f"could not retrieve pod logs: {log_error}")
                    break

                # show progress
                if i % 12 == 0:
                    if pod_name and i > 0:  # don't spam logs immediately
                        try:
                            logs = core_v1.read_namespaced_pod_log(name=pod_name, namespace="default")
                            if logs.strip():
                                print(f"current pod logs:\n{logs}")
                        except Exception:
                            pass

                time.sleep(2)

            except ApiException as e:
                if e.status == 404:
                    print(f"job {job_id} not found")
                else:
                    print(f"error checking job status: {e}")
                raise
        else:
            raise TimeoutError(f"job {job_id} did not complete within 10 minutes")

     
        print("waiting 4 seconds, just in case of like running lag, etc...")
        time.sleep(4)

        # Check S3 for results
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        )
        
        print(f"checking S3 for key: {result_key}")
        try:
            s3.head_object(Bucket=os.getenv("AWS_STORAGE_BUCKET_NAME"), Key=result_key)
          
            
            obj = s3.get_object(Bucket=os.getenv("AWS_STORAGE_BUCKET_NAME"), Key=result_key)
            content = obj["Body"].read().decode("utf-8")
            result_data = json.loads(content)
            
            if isinstance(result_data, dict) and "error" in result_data:
                if "traceback" in result_data:
                    print(f"error traceback:\n{result_data['traceback']}")
                
                try:
                    batch_v1.delete_namespaced_job(
                        name=job_id,
                        namespace="default",
                        body=client.V1DeleteOptions(propagation_policy="Foreground")
                    )
                    print(f"🧹 job {job_id} cleaned up")
                except Exception as cleanup_error:
                    print("could not delete job!")
                
                raise RuntimeError(f"algorithm did not work: {result_data['error']}")
            
            print("successfully retrieved result from s3")

            try:
                s3.delete_object(Bucket=os.getenv("AWS_STORAGE_BUCKET_NAME"), Key=result_key)
                print(f"🧹 Deleted result file from S3: {result_key}")
            except Exception as delete_error:
                print(f"⚠️ Warning: Failed to delete result file from S3: {delete_error}")
            
        except s3.exceptions.NoSuchKey:
          
            try:
                response = s3.list_objects_v2(
                    Bucket=os.getenv("AWS_STORAGE_BUCKET_NAME"),
                    Prefix="results/",
                    MaxKeys=10
                )
                if 'Contents' in response:
                    for obj in response['Contents']:
                        print("modified")
                else:
                    print("no files found in results/ directory")
            except Exception as list_error:
                print(f"could not list S3 objects: {list_error}")
            
           
            
            if job_failed:
                raise RuntimeError(f"Job {job_id} failed and no results were uploaded to S3")
            else:
                raise FileNotFoundError(f"Job completed but result file not found in S3: {result_key}")
            
        except Exception as e:
            raise

        try:
            batch_v1.delete_namespaced_job(
                name=job_id,
                namespace="default",
                body=client.V1DeleteOptions(propagation_policy="Foreground")
            )
        except ApiException as e:
            print(f"warning: Could not delete job {job_id}: {e}")

        return result_data

    except Exception as e:
        raise