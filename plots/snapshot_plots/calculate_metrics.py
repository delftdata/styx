import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib import rcParams
import pandas as pd
import numpy as np

plt.rcParams.update({'font.size': 20})
rcParams['figure.figsize'] = [8, 8]

n_keys = 1_000_000
# n_keys = 100
starting_money = 1_000_000

input_msgs = pd.read_csv('client_requests.csv', dtype={'request_id': bytes,
                                                       'timestamp': np.uint64}).sort_values('timestamp')
output_msgs = pd.read_csv('output.csv', dtype={'request_id': bytes,
                                               'timestamp': np.uint64}, low_memory=False).sort_values('timestamp')

output_run_messages = output_msgs

joined = pd.merge(input_msgs, output_msgs, on='request_id', how='outer')

runtime = joined['timestamp_y'] - joined['timestamp_x']
runtime_no_nan = runtime
print(f'min latency: {min(runtime_no_nan)}ms')
print(f'max latency: {max(runtime_no_nan)}ms')
print(f'average latency: {np.average(runtime_no_nan)}ms')
print(f'99%: {np.percentile(runtime_no_nan, 99)}ms')
print(f'95%: {np.percentile(runtime_no_nan, 95)}ms')
print(f'90%: {np.percentile(runtime_no_nan, 90)}ms')
print(f'75%: {np.percentile(runtime_no_nan, 75)}ms')
print(f'60%: {np.percentile(runtime_no_nan, 60)}ms')
print(f'50%: {np.percentile(runtime_no_nan, 50)}ms')
print(f'25%: {np.percentile(runtime_no_nan, 25)}ms')
print(f'10%: {np.percentile(runtime_no_nan, 10)}ms')
print(np.argmax(runtime_no_nan))
print(np.argmin(runtime_no_nan))

missed = joined[joined['response'].isna()]

if len(missed) > 0:
    print('--------------------')
    print('\nMISSED MESSAGES!\n')
    print('--------------------')
    print(missed)
    print('--------------------')
else:
    print('\nNO MISSED MESSAGES!\n')


snapshot_starts: dict[int, int] = {}
snapshot_ends: dict[int, int] = {}

with open('coordinator_logs.txt') as file:
    for line in file:
        split = line.split("WARNING:")[-1].split("|")
        worker_id = int(split[0].strip().split(" ")[-1])
        sn_id = int(split[1].strip().split(" ")[-1])
        start = int(round(float(split[2].strip().split(" ")[-1])))
        end = int(round(float(split[3].strip().split(" ")[-1])))
        total = split[4].strip().split(" ")[-1]
        print(worker_id, sn_id, start, end, total)
        if sn_id in snapshot_starts:
            snapshot_starts[sn_id] = min(snapshot_starts[sn_id], start)
        else:
            snapshot_starts[sn_id] = start
        if sn_id in snapshot_ends:
            snapshot_ends[sn_id] = max(snapshot_ends[sn_id], end)
        else:
            snapshot_ends[sn_id] = end


def update_ticks(x, pos):
    # if x == 0:
    #     return 'Mean'
    # elif pos == 6:
    #     return 'pos is 6'
    # else:
    #     return x
    return int(x / 10)


def update_throughput_ticks(x, pos):
    return int(x / 1000)

def update_throughput_ticks_float(x, pos):
    return round(x / 1000, 1)


def calculate_throughput(output_timestamps: list[int], granularity_ms: int,
                         sn_start: dict[int, int], sn_end: dict[int, int]):
    second_coefficient = 1000 / granularity_ms
    output_timestamps.sort()
    start_time = output_timestamps[0]
    end_time = output_timestamps[-1]
    bucket_boundaries = list(range(start_time, end_time, granularity_ms))
    bucket_boundaries = [(bucket_boundaries[i], bucket_boundaries[i + 1]) for i in range(len(bucket_boundaries) - 1)]
    bucket_counts: dict[int, int] = {i: 0 for i in range(len(bucket_boundaries))}
    bucket_counts_input: dict[int, int] = {i: 0 for i in range(len(bucket_boundaries))}
    for t in output_timestamps:
        for i, boundaries in enumerate(bucket_boundaries):
            if boundaries[0] <= t < boundaries[1]:
                bucket_counts[i] += 1 * second_coefficient

    for t in input_msgs['timestamp']:
        for i, boundaries in enumerate(bucket_boundaries):
            if boundaries[0] <= t < boundaries[1]:
                bucket_counts_input[i] += 1 * second_coefficient

    normalized_sn_starts: list[float] = []
    sn_starts = [sn_t for sn_t in sn_start.values() if start_time <= sn_t <= end_time]
    for t in sn_starts:
        for i, boundaries in enumerate(bucket_boundaries):
            if boundaries[0] <= t < boundaries[1]:
                normalized_sn_starts.append(((t - boundaries[0]) / granularity_ms) + i)

    normalized_sn_ends: list[float] = []
    sn_ends = [sn_t for sn_t in sn_end.values() if start_time <= sn_t <= end_time]
    for t in sn_ends:
        for i, boundaries in enumerate(bucket_boundaries):
            if boundaries[0] <= t < boundaries[1]:
                normalized_sn_ends.append(((t - boundaries[0]) / granularity_ms) + i)

    _, ax = plt.subplots()
    ax.plot(bucket_counts.values(), linewidth=2.5, label='Output Throughput')
    ax.plot(bucket_counts_input.values(), linewidth=2.5, label='Input Throughput', alpha=0.7)
    ax.vlines(normalized_sn_starts,
              ymin=0, ymax=15000, colors='green', linestyle='--', linewidth=3, label='Snapshot Start')
    ax.vlines(normalized_sn_ends,
              ymin=0, ymax=15000, colors='red', linestyle='dotted', linewidth=3, label='Snapshot End')
    # ax.axhline(y=3200, color='orange', linestyle='-')
    ax.set_xlabel('Time (seconds)')
    ax.set_ylabel('K Transactions per Second')
    ax.set_xlim([1.5, 6.5])
    ax.set_ylim([2100, 3900])
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(update_throughput_ticks_float))
    # ax.xaxis.set_major_formatter(mticker.FuncFormatter(update_ticks))
    ax.legend(bbox_to_anchor=(0.5, 1.08), loc="center", ncol=2)
    plt.grid(linestyle='--', linewidth=0.5)
    plt.savefig("throughput_snapshot.pdf")
    plt.show()
    return bucket_counts, normalized_sn_ends, normalized_sn_starts


bc, normalized_sn_ends, normalized_sn_starts = calculate_throughput(list(output_run_messages['timestamp']),
                                                                    1000,
                                                                    snapshot_starts,
                                                                    snapshot_ends)

timestamps = list(output_run_messages['timestamp'])
latencies = list(runtime)
start_time = timestamps[0]
end_time = timestamps[-1]
granularity_ms = 100
bucket_boundaries = list(range(start_time, end_time, granularity_ms))
bucket_boundaries = [(bucket_boundaries[i], bucket_boundaries[i + 1]) for i in range(len(bucket_boundaries) - 1)]

bucket_latencies: dict[int, list[float]] = {i: [] for i in range(len(bucket_boundaries))}

for i, t in enumerate(timestamps):
    for j, boundaries in enumerate(bucket_boundaries):
        if boundaries[0] <= t < boundaries[1]:
            bucket_latencies[j].append(latencies[i])

for k, v in bucket_latencies.items():
    if not v:
        bucket_latencies[k].append(np.nan)

bucket_latencies_99: dict[int, float] = {k: np.percentile(v, 99) for k, v in bucket_latencies.items() if v}
bucket_latencies_50: dict[int, float] = {k: np.percentile(v, 50) for k, v in bucket_latencies.items() if v}

normalized_sn_starts: list[float] = []
sn_starts = [sn_t for sn_t in snapshot_starts.values() if start_time <= sn_t <= end_time]
for t in sn_starts:
    for i, boundaries in enumerate(bucket_boundaries):
        if boundaries[0] <= t < boundaries[1]:
            normalized_sn_starts.append(((t - boundaries[0]) / granularity_ms) + i)

normalized_sn_ends: list[float] = []
sn_ends = [sn_t for sn_t in snapshot_ends.values() if start_time <= sn_t <= end_time]
for t in sn_ends:
    for i, boundaries in enumerate(bucket_boundaries):
        if boundaries[0] <= t < boundaries[1]:
            normalized_sn_ends.append(((t - boundaries[0]) / granularity_ms) + i)

# def update_latency_ticks(x, pos):
#     return x / 1000


_, ax = plt.subplots()
ax.plot(bucket_latencies_99.keys(), bucket_latencies_99.values(), linewidth=2.5, label='99p')
ax.plot(bucket_latencies_50.keys(), bucket_latencies_50.values(), linewidth=2.5, label='50p')
ax.vlines(normalized_sn_starts, ymin=0, ymax=1000,
          colors='green', linestyle='--', linewidth=3, label='Snapshot Start')
ax.vlines(normalized_sn_ends, ymin=0, ymax=1000,
          colors='red', linestyle='dotted', linewidth=3, label='Snapshot End')
ax.set_xlabel('Time (seconds)')
ax.set_ylabel('Latency (ms)')
ax.xaxis.set_major_formatter(mticker.FuncFormatter(update_ticks))
# ax.yaxis.set_major_formatter(mticker.FuncFormatter(update_latency_ticks))
ax.set_ylim([0, 40])
ax.set_xlim([15, 65])
plt.grid(linestyle='--', linewidth=0.5)
ax.legend(bbox_to_anchor=(0.5, 1.08), loc="center", ncol=2)
plt.savefig("latency_snapshot.pdf")
plt.show()
