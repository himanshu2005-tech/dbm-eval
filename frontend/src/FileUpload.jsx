import React, { useState } from "react";
import axios from "axios";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

// Utility: Ensures a value is a safe number (or "N/A" string for display)
const safeNumber = (value, decimals = 2) =>
  value !== undefined && value !== null && !isNaN(value)
    ? Number(value).toFixed(decimals)
    : "N/A";

// Utility: Safely divides two numbers, returns 0 if division is invalid
const safeDivide = (a, b) =>
  b && !isNaN(a / b) && b !== 0 ? a / b : 0;

// Utility: Ensures value is a number or null for Recharts consumption
const chartValue = (value) => 
    (value !== undefined && value !== null && !isNaN(value)) ? Number(value) : null;


export default function FileUpload() {
  const [file, setFile] = useState(null);
  const [response, setResponse] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleUpload = async () => {
    if (!file) return alert("Please select a file first.");
    const formData = new FormData();
    formData.append("file", file);
    setLoading(true);

    try {
      const res = await axios.post(
        "http://localhost:8000/upload-and-process",
        formData,
        {
          headers: { "Content-Type": "multipart/form-data" },
        }
      );
      setResponse(res.data);
    } catch (err) {
      console.error(err);
      alert("Upload failed! Please check backend.");
    } finally {
      setLoading(false);
    }
  };

  // === Data Transformation Functions (for Recharts) ===

  const performanceMetrics = (data) =>
    data
      ? [
          {
            name: "Execution Time (s)",
            scidb: chartValue(data.scidb?.execution_time_seconds),
            mapreduce: chartValue(data.mapreduce?.execution_time_seconds),
          },
          {
            name: "Memory (MB)",
            scidb: chartValue(safeDivide(data.scidb?.memory_usage_snapshot_mb, 1)),
            mapreduce: chartValue(safeDivide(data.mapreduce?.memory_usage_avg_mb, 1)),
          },
          {
            name: "CPU (%)",
            scidb: chartValue(data.scidb?.cpu_percent_snapshot),
            mapreduce: chartValue(data.mapreduce?.cpu_percent_avg),
          },
          {
            name: "Throughput (rows/s)",
            scidb: chartValue(data.scidb?.throughput_rows_per_sec),
            mapreduce: chartValue(data.mapreduce?.throughput_rows_per_sec),
          },
        ]
      : [];

  const dataMetrics = (data) =>
    data
      ? [
          {
            name: "Rows Processed",
            scidb: chartValue(data.scidb?.row_count),
            mapreduce: chartValue(data.mapreduce?.row_count),
          },
          {
            name: "Columns",
            scidb: chartValue(data.scidb?.column_count),
            mapreduce: chartValue(data.mapreduce?.column_count),
          },
          {
            name: "File Size (KB)",
            scidb: chartValue(safeDivide(data.scidb?.file_size_bytes, 1024)),
            mapreduce: chartValue(safeDivide(data.mapreduce?.file_size_bytes, 1024)),
          },
          {
            name: "Avg Row Size (bytes)",
            scidb: chartValue(data.scidb?.avg_row_size_bytes),
            mapreduce: chartValue(data.mapreduce?.avg_row_size_bytes),
          },
        ]
      : [];

  const systemMetrics = (data) =>
    data
      ? [
          {
            name: "Memory Usage (%)",
            scidb: chartValue(data.scidb?.memory_percent_snapshot),
            mapreduce: chartValue(data.mapreduce?.memory_percent_avg),
          },
        ]
      : [];

  // === Recharts Helper Components ===

  const CustomTooltip = ({ active, payload, label }) => {
    // Highly defensive check to prevent "cannot read properties of undefined (reading 'style')"
    if (active && payload && payload.length) {
      return (
        <div style={{ padding: '8px', border: '1px solid #ccc', backgroundColor: 'white' }}>
          <p>{label}</p>
          {payload.map((entry, index) => {
            if (!entry || entry.color === undefined) return null; // Final safety check

            return (
              <p key={index} style={{ color: entry.color }}>
                {entry.name}:{" "}
                {
                    typeof entry.value === "number" && !isNaN(entry.value) && entry.value !== null
                      ? entry.value.toFixed(2)
                      : "N/A"
                }
              </p>
            );
          })}
        </div>
      );
    }
    return null;
  };

  const renderChart = (data, title, colors = ["#4f46e5", "#10b981"]) => {
    // Filter out rows where both values are null/undefined for cleaner charts
    const filteredData = data.filter(item => 
        item.scidb !== null || item.mapreduce !== null
    );

    return (
      <div style={{ margin: '20px 0', border: '1px solid #eee', padding: '15px' }}>
        <h4>{title}</h4>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={filteredData} margin={{ top: 20, right: 30, left: 20, bottom: 60 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" angle={-45} textAnchor="end" height={80} />
            <YAxis />
            <Tooltip content={<CustomTooltip />} />
            <Legend />
            <Bar dataKey="scidb" fill={colors[0]} />
            <Bar dataKey="mapreduce" fill={colors[1]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    );
  };

  const MetricCard = ({ title, scidbValue, mapreduceValue, format = (v) => v }) => (
    <div style={{ border: '1px solid #ccc', padding: '10px', margin: '5px', flex: 1 }}>
      <h5>{title}</h5>
      <p>SciDB: {format(scidbValue)}</p>
      <p>MapReduce: {format(mapreduceValue)}</p>
    </div>
  );

  // === Component Logic ===

  const perfData = performanceMetrics(response);
  const dataStats = dataMetrics(response);
  const sysStats = systemMetrics(response);

  return (
    <div style={{ maxWidth: '1200px', margin: '40px auto', padding: '20px' }}>
      <h2>File Upload & Database Benchmark</h2>

      <div style={{ display: 'flex', gap: '10px', margin: '15px 0' }}>
        <input
          type="file"
          onChange={(e) => setFile(e.target.files[0])}
        />
        <button
          onClick={handleUpload}
          disabled={loading}
        >
          {loading ? "Processing..." : "Upload & Process"}
        </button>
      </div>

      {response && (
        <div style={{ marginTop: '30px' }}>
          <h3>Performance Summary</h3>

          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '20px' }}>
            <MetricCard
              title="Execution Time"
              scidbValue={safeNumber(response.scidb?.execution_time_seconds, 3)}
              mapreduceValue={safeNumber(response.mapreduce?.execution_time_seconds, 3)}
              format={(v) => `${v}s`}
            />
            <MetricCard
              title="Throughput"
              scidbValue={safeNumber(response.scidb?.throughput_rows_per_sec, 0)}
              mapreduceValue={safeNumber(response.mapreduce?.throughput_rows_per_sec, 0)}
              format={(v) => `${v} rows/s`}
            />
            <MetricCard
              title="Memory Usage"
              scidbValue={safeNumber(response.scidb?.memory_usage_snapshot_mb, 1)}
              mapreduceValue={safeNumber(response.mapreduce?.memory_usage_avg_mb, 1)}
              format={(v) => `${v} MB`}
            />
            <MetricCard
              title="CPU Usage"
              scidbValue={safeNumber(response.scidb?.cpu_percent_snapshot, 1)}
              mapreduceValue={safeNumber(response.mapreduce?.cpu_percent_avg, 1)}
              format={(v) => `${v}%`}
            />
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '20px' }}>
            {renderChart(perfData, "Performance Metrics")}
            {renderChart(dataStats, "Data Metrics")}
            {renderChart(sysStats, "System Metrics")}
          </div>

          <h3>Detailed Metrics</h3>
          <div style={{ display: 'flex', gap: '20px' }}>
            <div style={{ flex: 1, border: '1px solid blue', padding: '15px' }}>
              <h4>SciDB Details</h4>
              {Object.entries(response.scidb || {}).map(([key, val]) => (
                <p key={key}><strong>{key.replace(/_/g, " ")}:</strong> {typeof val === "object" ? JSON.stringify(val) : String(val)}</p>
              ))}
            </div>

            <div style={{ flex: 1, border: '1px solid green', padding: '15px' }}>
              <h4>MapReduce Details</h4>
              {Object.entries(response.mapreduce || {}).map(([key, val]) => (
                <p key={key}><strong>{key.replace(/_/g, " ")}:</strong> {typeof val === "object" ? JSON.stringify(val) : String(val)}</p>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}