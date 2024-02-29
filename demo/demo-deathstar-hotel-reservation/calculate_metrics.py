import json
import math
import sys

import pandas as pd
import numpy as np


SAVE_DIR: str = sys.argv[1]
input_rate = int(sys.argv[2])

n_keys = 888
warmup_seconds = int(sys.argv[3])
client_threads = int(sys.argv[4])

exp_name = f"d_hotel_reservation_{input_rate * client_threads}"

origin_input_msgs = pd.read_csv(f'{SAVE_DIR}/client_requests.csv',
                                dtype={'request_id': bytes,
                                       'timestamp': np.uint64}).sort_values('timestamp')

duplicate_requests = not origin_input_msgs['request_id'].is_unique


output_msgs = pd.read_csv(f'{SAVE_DIR}/output.csv',
                          dtype={'request_id': bytes,
                                 'timestamp': np.uint64}, low_memory=False).sort_values('timestamp')

exactly_once_output = output_msgs['request_id'].is_unique

output_run_messages = output_msgs.iloc[n_keys:]

# remove warmup from input
input_msgs = origin_input_msgs.loc[(origin_input_msgs['timestamp'] -
                                    origin_input_msgs['timestamp'][0] >= warmup_seconds * 1000)]

joined = pd.merge(input_msgs, output_run_messages, on='request_id', how='outer')
missed = len(joined[joined['response'].isna()])

joined = joined.dropna()
runtime = joined['timestamp_y'] - joined['timestamp_x']


start_time = -math.inf
throughput = {}
bucket_id = -1

# 1 second (ms) (i.e. bucket size)
granularity = 1000

for t in output_msgs['timestamp']:
    if t - start_time > granularity:
        bucket_id += 1
        start_time = t
        throughput[bucket_id] = 1
    else:
        throughput[bucket_id] += 1

throughput_vals = list(throughput.values())

req_ids = output_msgs['request_id']
dup = output_msgs[req_ids.isin(req_ids[req_ids.duplicated()])].sort_values("request_id")


res_dict = {
    "duplicate_requests": duplicate_requests,
    "exactly_once_output": exactly_once_output,
    "latency (ms)": {10: np.percentile(runtime, 10),
                     20: np.percentile(runtime, 20),
                     30: np.percentile(runtime, 30),
                     40: np.percentile(runtime, 40),
                     50: np.percentile(runtime, 50),
                     60: np.percentile(runtime, 60),
                     70: np.percentile(runtime, 70),
                     80: np.percentile(runtime, 80),
                     90: np.percentile(runtime, 90),
                     95: np.percentile(runtime, 95),
                     99: np.percentile(runtime, 99),
                     "max": max(runtime),
                     "min": min(runtime),
                     "mean": np.average(runtime)
                     },
    "missed messages": missed,
    "throughput": {
        "max": max(throughput_vals),
        "avg": sum(throughput_vals) / len(throughput_vals),
        "TPS": throughput_vals
    },
    "duplicate_messages": len(dup)
}

with open(f'{SAVE_DIR}/{exp_name}.json', 'w', encoding='utf-8') as f:
    json.dump(res_dict, f, ensure_ascii=False, indent=4)
