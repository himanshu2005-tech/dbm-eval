from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import subprocess
import os
import time
import psutil

app = FastAPI()

class ImportRequest(BaseModel):
    csv_path: str
    array_name: str

DOCKER_CONTAINER_NAME = "scidb_container"

def run_command_in_docker(cmd: list) -> str:
    docker_cmd = ['docker', 'exec', '-i', DOCKER_CONTAINER_NAME] + cmd
    result = subprocess.run(docker_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Error: {result.stderr.strip()}")
    return result.stdout.strip()

def map_windows_path_to_docker_path(win_path: str) -> str:
    host_mount = "C:/Users/vigne/OneDrive/Desktop/academic_work/dbms_project/dbm-eval/backend/uploads"
    container_mount = "/data"

    win_path_normalized = win_path.replace("\\", "/")
    if win_path_normalized.startswith(host_mount):
        docker_path = win_path_normalized.replace(host_mount, container_mount)
        return docker_path
    else:
        raise ValueError(f"CSV path {win_path} is outside of the mounted folder {host_mount}")

def monitor_query_performance(cmd: list):
    proc = psutil.Process()
    cpu_before = proc.cpu_percent(interval=None)
    mem_before = proc.memory_info().rss  # in bytes

    start = time.perf_counter()
    output = run_command_in_docker(cmd)
    duration = time.perf_counter() - start

    cpu_after = proc.cpu_percent(interval=None)
    mem_after = proc.memory_info().rss

    cpu_used = cpu_after - cpu_before
    mem_used = mem_after - mem_before

    return output, duration, cpu_used, mem_used

def import_csv_to_scidb(csv_path: str, array_name: str) -> None:
    create_cmd = ['iquery', '-aq', f"create array {array_name} <x:string>[i=0:*]"]
    run_command_in_docker(create_cmd)

    docker_csv_path = map_windows_path_to_docker_path(csv_path)
    load_cmd = ['iquery', '-aq', f"load({array_name}, '{docker_csv_path}', -2, 'csv')"]
    run_command_in_docker(load_cmd)

def run_generic_queries(array_name: str):
    queries = {
        "count_rows": f"aggregate({array_name}, count(*))",
        "project_x": f"project({array_name}, x)",
        "non_null_x": f"filter({array_name}, x != '')",
        "sample_10": f"between({array_name}, 0, 9)"
    }
    results = {}
    for name, query in queries.items():
        try:
            query_cmd = ['iquery', '-aq', query]
            output, duration, cpu_used, mem_used = monitor_query_performance(query_cmd)
            results[name] = {
                "query": query,
                "result": output,
                "execution_time_seconds": duration,
                "cpu_percent_change": cpu_used,
                "memory_rss_change_bytes": mem_used
            }
        except Exception as e:
            results[name] = {"query": query, "error": str(e)}
    return results

@app.post("/import-and-query")
async def import_and_query(request: ImportRequest):
    if not os.path.isfile(request.csv_path):
        raise HTTPException(status_code=400, detail="CSV file path does not exist")
    try:
        import_csv_to_scidb(request.csv_path, request.array_name)
        query_results = run_generic_queries(request.array_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": "CSV imported and generic queries executed", "results": query_results}
