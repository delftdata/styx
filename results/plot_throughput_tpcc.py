import ast

from matplotlib import rcParams
import matplotlib.pyplot as plt
import pandas as pd

rcParams["figure.figsize"] = [14, 5]
plt.rcParams.update({"font.size": 22})
warmup_seconds = 30
interval_size = 1
start_migration_time = 30
end_migration_time = 88


# Load CSVs
input_df = pd.read_csv("client_requests.csv")
output_df = pd.read_csv("output.csv")

# Parse request_id byte strings
input_df["request_id"] = input_df["request_id"].apply(ast.literal_eval)
output_df["request_id"] = output_df["request_id"].apply(ast.literal_eval)

# Normalize timestamps to start from 0 seconds
t0 = min(input_df["timestamp"].min(), output_df["timestamp"].min())
input_df["time_since_start_sec"] = (input_df["timestamp"] - t0) / 1000
output_df["time_since_start_sec"] = (output_df["timestamp"] - t0) / 1000

# Filter out warmup
input_df_filtered = input_df[input_df["time_since_start_sec"] >= warmup_seconds].copy()
output_df_filtered = output_df[output_df["time_since_start_sec"] >= warmup_seconds].copy()

# Floor to time bucket
input_df_filtered["time_bucket"] = (input_df_filtered["time_since_start_sec"] // interval_size) * interval_size
output_df_filtered["time_bucket"] = (output_df_filtered["time_since_start_sec"] // interval_size) * interval_size

# Count requests per bucket
throughput_in_df = input_df_filtered.groupby("time_bucket").size().reset_index(name="throughput_in")
throughput_out_df = output_df_filtered.groupby("time_bucket").size().reset_index(name="throughput_out")

# Merge and fill gaps
throughput_df = pd.merge(throughput_in_df, throughput_out_df, on="time_bucket", how="outer").fillna(0)

# Shift x-axis to start from 0 post-warmup
throughput_df["time_bucket_shifted"] = throughput_df["time_bucket"] - warmup_seconds

# Plot throughput
plt.plot(throughput_df["time_bucket_shifted"], throughput_df["throughput_in"], label="Input Throughput", linewidth=3)
plt.plot(throughput_df["time_bucket_shifted"], throughput_df["throughput_out"], label="Output Throughput", linewidth=3, alpha=0.8)
plt.axvline(x=start_migration_time, color="red", linestyle="--", label="Start Migration", linewidth=3)
plt.text(start_migration_time - 7, 1250, f"{start_migration_time}s", color="red", fontsize=20, ha="center", va="top")
plt.axvline(x=end_migration_time, color="green", linestyle="--", label="End Migration", linewidth=3)
plt.text(end_migration_time + 7, 1250, f"{end_migration_time}s", color="green", fontsize=20, ha="center", va="top")
plt.xlabel("Time (s)")
plt.grid(linestyle="dotted", linewidth=1.5, axis="y")
plt.ylabel("Throughput (TPS)")
plt.legend()
plt.ylim([0, 30000])
plt.xlim([0, 300])
plt.tight_layout()
plt.savefig("throughput_tpcc.pdf")
plt.show()
