import math
import re

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


input_msgs = pd.read_csv('client_requests.csv', dtype={'request_id': np.uint64,
                                                       'timestamp': np.uint64}).sort_values('timestamp')
output_msgs = pd.read_csv('output.csv', dtype={'request_id': np.uint64,
                                               'timestamp': np.uint64}, low_memory=False).sort_values('timestamp')

init_messages = output_msgs.head(n_keys)
verif_messages = output_msgs.tail(n_keys)
output_run_messages = output_msgs.iloc[n_keys:len(output_msgs)-n_keys]

joined = pd.merge(input_msgs, output_run_messages, on='request_id', how='outer')

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


def update_ticks(x, pos):
    return int(x / 10)


def update_throughput_ticks(x, pos):
    return int(x / 1000)


def calculate_throughput(output_timestamps: list[int], granularity_ms: int):
    second_coefficient = 1000 / granularity_ms
    output_timestamps.sort()
    start_time = output_timestamps[0]
    end_time = output_timestamps[-1]
    bucket_boundaries = list(range(start_time, end_time, granularity_ms))
    bucket_boundaries = [(bucket_boundaries[i], bucket_boundaries[i + 1]) for i in range(len(bucket_boundaries)-1)]
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

    _, ax = plt.subplots()
    ax.plot(bucket_counts.values(), linewidth=2.5, label='Output Throughput')
    ax.plot(bucket_counts_input.values(), label='Input Throughput', linewidth=2.5, alpha=0.7)
    ax.vlines([(1680277756328 - start_time)/100], ymin=0, ymax=12500,
              colors='red', linestyle='--', linewidth=3, label='Worker Failure Detected')
    ax.vlines([159], ymin=0, ymax=12500,
              colors='green', linestyle='--', linewidth=3, label='Recovery Complete')
    # ax.axhline(y=3200, color='orange', linestyle='-')
    ax.set_xlabel('Time (seconds)')
    ax.set_ylabel('K Transactions per Second')
    ax.set_ylim([0, 12500])
    ax.set_xlim([110, 190])
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(update_ticks))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(update_throughput_ticks))
    ax.legend(bbox_to_anchor=(0.5, 1.08), loc="center", ncol=2, fontsize=18)
    plt.grid(linestyle='--', linewidth=0.5)
    plt.savefig("throughput_recovery.pdf")
    plt.show()
    return bucket_counts


bc = calculate_throughput(list(output_run_messages['timestamp']),
                          100)

print(bc)

req_ids = output_msgs['request_id']
dup = output_msgs[req_ids.isin(req_ids[req_ids.duplicated()])].sort_values("request_id")

print(f'Number of input messages: {len(input_msgs)}')
print(f'Number of output messages: {len(output_msgs) - 2 * n_keys}')
print(f'Number of duplicate messages: {len(dup)}')

if len(dup) > 0:
    print('--------------------')
    print('\nDUPLICATE MESSAGES!\n')
    print('--------------------')
    print(dup)
    print('--------------------')
else:
    print('\nNO DUPLICATE MESSAGES!\n')


# Consistency test
verification_state_reads = {int(e[0]): int(e[1]) for e in [res.strip('][').split(', ')
                                                           for res in output_msgs['response'].tail(n_keys)]}


transaction_operations = [(int(op[0]), int(op[1])) for op in [op.split(' ')[1].split('->') for op in input_msgs['op']]]
true_res = {key: starting_money for key in range(n_keys)}

for op in transaction_operations:
    send_key, rcv_key = op
    true_res[send_key] -= 1
    true_res[rcv_key] += 1

print(f'Are we consistent: {true_res == verification_state_reads}')

missing_verification_keys = []
for res in true_res.items():
    key, value = res
    if key in verification_state_reads and verification_state_reads[key] != value:
        print(f'For key: {key} the value should be {value} but it is {verification_state_reads[key]}')
    elif key not in verification_state_reads:
        missing_verification_keys.append(key)

print(f'\nMissing {len(missing_verification_keys)} keys in the verification')

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


def update_latency_ticks(x, pos):
    return x / 1000


_, ax = plt.subplots()
ax.plot(bucket_latencies_99.keys(), bucket_latencies_99.values(), linewidth=2.5, label='99p')
ax.plot(bucket_latencies_50.keys(), bucket_latencies_50.values(), linewidth=2.5, label='50p')
ax.vlines([(1680277756328 - start_time) / 100], ymin=0, ymax=3000,
          colors='red', linestyle='--', linewidth=3, label='Worker Failure Detected')
ax.vlines([159], ymin=0, ymax=3000,
          colors='green', linestyle='--', linewidth=3, label='Recovery Complete')
ax.set_xlabel('Time (seconds)')
ax.set_ylabel('Latency (seconds)')
ax.xaxis.set_major_formatter(mticker.FuncFormatter(update_ticks))
ax.yaxis.set_major_formatter(mticker.FuncFormatter(update_latency_ticks))
ax.set_ylim([0, 3000])
ax.set_xlim([110, 190])
plt.grid(linestyle='--', linewidth=0.5)
ax.legend(bbox_to_anchor=(0.5, 1.08), loc="center", ncol=2, fontsize=18)
plt.savefig("latency_recovery.pdf")
plt.show()
