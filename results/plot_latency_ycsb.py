import ast

from matplotlib import rcParams
import matplotlib.pyplot as plt
import pandas as pd

rcParams["figure.figsize"] = [14, 5]
plt.rcParams.update({"font.size": 22})
warmup_seconds = 30

start_migration_time_ms_epoch  = 1747337077616 - 1000
end_migration_time_ms_epoch = 1747337108856


# Load CSVs
input_df = pd.read_csv("client_requests.csv")
output_df = pd.read_csv("output.csv")

# Parse request_id byte strings
input_df["request_id"] = input_df["request_id"].apply(ast.literal_eval)
output_df["request_id"] = output_df["request_id"].apply(ast.literal_eval)

# Join on request_id
merged_df = pd.merge(input_df, output_df, on="request_id", suffixes=("_in", "_out"))

# Compute latency in milliseconds
merged_df["latency_ms"] = merged_df["timestamp_out"] - merged_df["timestamp_in"]

# Normalize timestamps to start from 0 seconds
t0 = merged_df["timestamp_in"].min()
merged_df["time_since_start_sec"] = (merged_df["timestamp_in"] - t0) / 1000

end_migration_time = ((end_migration_time_ms_epoch - t0) // 1000) - warmup_seconds
start_migration_time = ((start_migration_time_ms_epoch - t0) // 1000) - warmup_seconds

# Filter to show only 5 seconds onwards
filtered_df = merged_df[merged_df["time_since_start_sec"] >= warmup_seconds].sort_values(by="time_since_start_sec")

# Define interval size in seconds
interval_size = 1

# Floor the time to the nearest interval
filtered_df["time_bucket"] = (filtered_df["time_since_start_sec"] // interval_size) * interval_size

# Compute mean latency per bucket
mean_latency_df = filtered_df.groupby("time_bucket")["latency_ms"].mean().reset_index()

# Shift x-axis to start from 0 after warmup
mean_latency_df["time_bucket_shifted"] = mean_latency_df["time_bucket"] - warmup_seconds

# Plot mean latency
plt.plot(mean_latency_df["time_bucket_shifted"], mean_latency_df["latency_ms"], label="Mean Latency", linewidth=3)
plt.axvline(x=start_migration_time, color="red", linestyle="--", label="Start Migration", linewidth=3)
plt.text(start_migration_time - 3, -5, f"{start_migration_time}s", color="red", fontsize=20, ha="center", va="top")
plt.axvline(x=end_migration_time, color="green", linestyle="--", label="End Migration", linewidth=3)
plt.text(end_migration_time + 3, -5, f"{end_migration_time}s", color="green", fontsize=20, ha="center", va="top")
plt.xlabel("Time (s)")
plt.grid(linestyle="dotted", linewidth=1.5, axis="y")
plt.ylabel("Latency (ms)")
plt.legend()
plt.ylim([0, 12000])
plt.xlim([0, 120])
plt.tight_layout()
plt.savefig("latency_ycsb.pdf")
plt.show()
