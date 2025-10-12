def compare_metrics(scidb_metrics, mapreduce_metrics):
    if "error" in scidb_metrics or "error" in mapreduce_metrics:
        raise ValueError("One or both systems failed to upload the file properly")

    return {
        "faster_system": (
            "SciDB" if scidb_metrics["execution_time_seconds"] < mapreduce_metrics["execution_time_seconds"]
            else "MapReduce"
        ),
        "execution_time_diff": abs(scidb_metrics["execution_time_seconds"] - mapreduce_metrics["execution_time_seconds"]),
        "cpu_diff": abs(scidb_metrics["cpu_percent_change"] - mapreduce_metrics["cpu_percent_change"]),
        "memory_diff": abs(scidb_metrics["memory_rss_change_bytes"] - mapreduce_metrics["memory_rss_change_bytes"]),
        "disk_read_diff": abs(scidb_metrics.get("disk_read_bytes", 0) - mapreduce_metrics.get("disk_read_bytes", 0)),
        "disk_write_diff": abs(scidb_metrics.get("disk_write_bytes", 0) - mapreduce_metrics.get("disk_write_bytes", 0)),
    }
