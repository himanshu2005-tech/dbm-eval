# mapreduce.py
import os
import shutil
import time
import psutil
import hashlib
import platform
import subprocess
import csv
import sys

def calculate_file_metadata(csv_path: str):
    """Calculates file size, MD5 hash, row count, and column count safely."""
    try:
        file_size = os.path.getsize(csv_path)
    except FileNotFoundError:
        return 0, None, 0, 0

    hash_md5 = hashlib.md5()
    with open(csv_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    file_md5 = hash_md5.hexdigest()

    row_count = 0
    column_count = 0
    
    with open(csv_path, 'r', newline='') as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
            column_count = len(header)
            row_count = sum(1 for row in reader)
        except StopIteration:
            row_count = 0
            column_count = 0
            
    return file_size, file_md5, row_count, column_count


def upload_to_mapreduce(csv_path: str):
    """Execute a MapReduce job simulation and collect real metrics."""
    start_time = time.perf_counter()

    # 1. Prepare Storage and Metadata
    storage_folder = os.path.join(os.path.dirname(csv_path), "mapreduce_storage")
    os.makedirs(storage_folder, exist_ok=True)
    stored_file_path = os.path.join(storage_folder, os.path.basename(csv_path))
    shutil.copy(csv_path, stored_file_path)

    file_size, file_md5, row_count, column_count = calculate_file_metadata(stored_file_path)

    # 2. MapReduce Job Execution (INLINE SIMULATION)
    # The code is structured to ensure at least 8.0 seconds of measurable activity (CPU/Memory load)
    simulation_code = f"""
import time, random, sys, os

# Set the path variable safely
path = '{stored_file_path.replace(os.path.sep, '/')}' 

# Simulate I/O and Memory Allocation (reading and holding data)
try:
    with open(path, 'r') as f: lines = f.readlines()
except: lines = []

# Simulate CPU Work
start = time.perf_counter()
result = 0
for i in range(200000000): # Increased CPU cycles
    result += (random.random() * random.random()) % 100

# Hold memory load to maintain RSS size
memory_load = [list(range(500)) for _ in range(10000)] # Increased memory load

exec_time = time.perf_counter() - start

# Critical: Enforce a minimum runtime for stable metric sampling
MIN_RUN_TIME = 8.0
if exec_time < MIN_RUN_TIME: 
    time.sleep(MIN_RUN_TIME - exec_time)

sys.exit(0)
"""
    
    # Use sys.executable and the -c flag to run the simulation code inline
    command = [sys.executable, "-c", simulation_code]
    
    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    try:
        ps_proc = psutil.Process(proc.pid)
    except psutil.NoSuchProcess:
        stdout, stderr = proc.communicate()
        raise Exception(f"MapReduce Job Failed on Start. STDERR: {stderr.decode()}")

    # 3. Collect Real-Time Metrics
    cpu_samples = []
    mem_samples = []

    while proc.poll() is None:
        try:
            # Sample CPU/Memory at a high rate (0.1s)
            cpu_samples.append(ps_proc.cpu_percent(interval=0.1))
            mem_samples.append(ps_proc.memory_info().rss / (1024 * 1024))
        except psutil.NoSuchProcess:
            break
            
    # CRITICAL: Wait for the process to fully close and collect final logs
    stdout, stderr = proc.communicate() 
    
    end_time = time.perf_counter()
    exec_time = end_time - start_time
    
    # 4. Final Calculations
    if proc.returncode != 0:
        print(f"❌ MapReduce Job Failed! Execution logs:\n{stderr.decode()}")

    avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0.0
    avg_mem = sum(mem_samples) / len(mem_samples) if mem_samples else 0.0
    
    # Throughput calculation
    throughput_rows_per_sec = row_count / exec_time if row_count and exec_time else 0.0
    avg_row_size_bytes = file_size / (row_count if row_count else 1)

    # System / Environment Info
    system_info = {
        "os_name": platform.system(),
        "os_version": platform.version(),
        "architecture": platform.machine(),
        "python_version": platform.python_version(),
        "cpu_model": platform.processor(),
    }

    # 5. Return Results
    return {
        "stored_file": stored_file_path,
        "execution_time_seconds": exec_time,
        "cpu_percent_avg": avg_cpu,
        "memory_usage_avg_mb": avg_mem,
        "row_count": row_count,
        "column_count": column_count,
        "throughput_rows_per_sec": throughput_rows_per_sec,
        "avg_row_size_bytes": avg_row_size_bytes,
        "file_size_bytes": file_size,
        "file_md5": file_md5,
        "system_info": system_info
    }


if __name__ == '__main__':
    # --- Self-Test Block ---
    dummy_csv = "test_data_mr.csv"
    if not os.path.exists(dummy_csv):
        print(f"Creating dummy file: {dummy_csv}")
        with open(dummy_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['ID', 'Value', 'Timestamp'])
            for i in range(5000):
                writer.writerow([i, f"Data_{i}", time.time()])

    print("\n--- Starting MapReduce Simulation ---")
    try:
        metrics = upload_to_mapreduce(dummy_csv)
        if metrics:
            print("\n✅ MapReduce Simulation Metrics:")
            for key, value in metrics.items():
                if key != "system_info":
                    print(f"  {key}: {value}")
            print("  System Info:", metrics["system_info"])
    except Exception as e:
        print(f"An error occurred: {e}")