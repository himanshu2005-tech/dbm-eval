# scidb.py
import os
import subprocess
import time
import hashlib
import platform
import json
import csv

def md5(fname):
    """Compute MD5 hash for a given file by reading in chunks."""
    hash_md5 = hashlib.md5()
    try:
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
    except FileNotFoundError:
        return None
    return hash_md5.hexdigest()

def get_docker_stats(container_name):
    """Get real-time stats of a Docker container using the stats command."""
    try:
        # Check if the container is running and get a snapshot of its resources
        stats = subprocess.run(
            ["docker", "stats", container_name, "--no-stream", "--format", "{{json .}}"],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Parse the last valid JSON line
        json_output = [line for line in stats.stdout.strip().split('\n') if line.startswith('{')][-1]
        data = json.loads(json_output)

        # Parsing Memory Usage and converting to MiB
        mem_raw = data["MemUsage"].split("/")[0].strip()
        unit = 1024 if "GiB" in mem_raw else 1 # Conversion factor to MiB
        mem_val = float(mem_raw.replace("MiB", "").replace("GiB", "").strip())
        mem_usage_mb = mem_val * unit

        return {
            "cpu_percent": float(data["CPUPerc"].replace("%", "")),
            "mem_usage_mb": mem_usage_mb,
            "mem_percent": float(data["MemPerc"].replace("%", "")),
            "net_io": data["NetIO"],
            "block_io": data["BlockIO"]
        }
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Docker Stats Failed: Container '{container_name}' is likely not running, named differently, or the Docker daemon is down. Error: {e.stderr.decode().strip()}")
        return None
    except Exception as e:
        print(f"⚠️ Failed to parse Docker stats output: {e}")
        return None


def calculate_csv_metadata(csv_path: str):
    """Safely calculate row and column count for a CSV."""
    row_count = 0
    column_count = 0
    try:
        with open(csv_path, 'r', newline='') as f:
            reader = csv.reader(f)
            header = next(reader)
            column_count = len(header)
            row_count = sum(1 for row in reader)
    except Exception:
        pass
    return row_count, column_count


def upload_to_scidb(csv_path: str):
    """Uploads a CSV file to SciDB Docker container and collects system metrics."""
    # Assumes the host machine's CSV file is mounted to the container's /data folder
    docker_csv_path = f"/data/{os.path.basename(csv_path)}"
    array_name = "csv_data"
    container_name = "scidb_container" # CRITICAL: Ensure your running container has this name

    # 1. File characteristics (from host machine)
    file_size = os.path.getsize(csv_path)
    file_md5 = md5(csv_path)
    row_count, column_count = calculate_csv_metadata(csv_path)

    try:
        # 2. SciDB setup and load
        
        # Clean up previous array if exists
        subprocess.run(
            ["docker", "exec", container_name, "iquery", "-aq", f"remove({array_name})"],
            capture_output=True
        )

        # Create a basic array
        subprocess.run(
            ["docker", "exec", container_name, "iquery", "-aq",
             f"create array {array_name} <val:string>[i=0:*:0:100000]"],
            check=True, capture_output=True
        )

        # Measure load query time only
        start_query = time.perf_counter()
        subprocess.run(
            ["docker", "exec", container_name, "iquery", "-naq",
             f"load({array_name}, '{docker_csv_path}', -2, 'csv')"],
            check=True, capture_output=True
        )
        exec_time = time.perf_counter() - start_query

        # 3. Retrieve SciDB Docker container metrics
        scidb_metrics = get_docker_stats(container_name)
        
        # Get actual row count from SciDB to confirm load
        result = subprocess.run(
            ["docker", "exec", container_name, "iquery", "-otsv", "-aq",
             f"aggregate({array_name}, count(*))"],
            capture_output=True,
            text=True
        )
        actual_row_count = row_count
        if result.stdout:
            try:
                # Attempt to parse the actual row count from the iquery TSV output
                actual_row_count = int(result.stdout.strip().split('\n')[-1].split('\t')[-1].strip())
            except Exception:
                pass

        # 4. Derived metrics
        throughput_rows_per_sec = actual_row_count / exec_time if actual_row_count and exec_time else 0.0
        avg_row_size_bytes = file_size / (actual_row_count or 1)

        system_info = {
            "os_name": platform.system(),
            "os_version": platform.version(),
            "architecture": platform.machine(),
            "python_version": platform.python_version(),
            "cpu_model": platform.processor(),
        }

        # 5. Return Results (return 0.0 for failed/missing Docker stats)
        return {
            "stored_file": docker_csv_path,
            "execution_time_seconds": exec_time,
            "cpu_percent_snapshot": scidb_metrics["cpu_percent"] if scidb_metrics else 0.0,
            "memory_usage_snapshot_mb": scidb_metrics["mem_usage_mb"] if scidb_metrics else 0.0,
            "memory_percent_snapshot": scidb_metrics["mem_percent"] if scidb_metrics else 0.0,
            "row_count": actual_row_count,
            "column_count": column_count,
            "file_size_bytes": file_size,
            "file_md5": file_md5,
            "throughput_rows_per_sec": throughput_rows_per_sec,
            "avg_row_size_bytes": avg_row_size_bytes,
            "docker_net_io": scidb_metrics["net_io"] if scidb_metrics else None,
            "docker_block_io": scidb_metrics["block_io"] if scidb_metrics else None,
            "system_info": system_info
        }

    except subprocess.CalledProcessError as e:
        raise Exception(f"SciDB command failed. Check SciDB setup in the Docker container. STDOUT: {e.stdout.decode()} | STDERR: {e.stderr.decode()}")
    except FileNotFoundError:
        raise Exception(f"SciDB command failed. Is Docker installed and accessible?")


if __name__ == '__main__':
    # --- Self-Test Block ---
    dummy_csv = "test_data_scidb.csv"
    if not os.path.exists(dummy_csv):
        print(f"Creating dummy file: {dummy_csv}")
        with open(dummy_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['ID', 'Value', 'Timestamp'])
            for i in range(100):
                writer.writerow([i, f"Data_{i}", time.time()])

    print("\n--- Starting SciDB Upload Test ---")
    print("NOTE: This requires Docker to be running and a container named 'scidb_container' to be active.")
    try:
        metrics = upload_to_scidb(dummy_csv) 
        if metrics:
            print("\n✅ SciDB Metrics:")
            for key, value in metrics.items():
                if key != "system_info":
                    print(f"  {key}: {value}")
            print("  System Info:", metrics["system_info"])
    except Exception as e:
        print(f"An error occurred: {e}")