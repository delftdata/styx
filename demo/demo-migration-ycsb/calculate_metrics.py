import json
import math

import pandas as pd
import numpy as np


def main(
        input_rate,
        client_threads,
        warmup_seconds,
        save_dir,
):
    print('Calculating metrics...')

    exp_name = f"ycsb_migration_{input_rate * client_threads}"

    origin_input_msgs = pd.read_csv(f'{save_dir}/client_requests.csv',
                                    dtype={'request_id': bytes,
                                        'timestamp': np.uint64}).sort_values('timestamp')

    duplicate_requests = not origin_input_msgs['request_id'].is_unique


    output_msgs = pd.read_csv(f'{save_dir}/output.csv', dtype={'request_id': bytes,
                                                            'timestamp': np.uint64},
                            low_memory=False).sort_values('timestamp')
    print(
        f'Found {len(output_msgs)} output messages with {len(output_msgs['response'].dropna())} non-empty responses.'
    )

    exactly_once_output = output_msgs['request_id'].is_unique

    # remove warmup from input
    input_msgs = origin_input_msgs.loc[(origin_input_msgs['timestamp'] -
                                        origin_input_msgs['timestamp'][0] >= warmup_seconds * 1000)]
    print(
        f'Removed {len(origin_input_msgs) - len(input_msgs)} input messages '
        f'due to an initial warmup period of {warmup_seconds} seconds.'
    )

    # Perform a left join to include all rows from client_df
    joined = input_msgs.merge(output_msgs, on='request_id', how='left', suffixes=('_client', '_output'))

    print(
        f'Joined {len(joined)} messages from {len(input_msgs)} input messages '
        f'and {len(output_msgs)} output messages.'
    )
    missed_requests = joined[joined['timestamp_output'].isna()]
    print(f"missed_requests: {missed_requests}")
    missed = len(missed_requests)
    print(
        f'Missed {missed} messages after the join for which no response '
         'was found.'
    )

    joined = joined.dropna()
    runtime = joined['timestamp_output'] - joined['timestamp_client']

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
        "messages": missed,
        "throughput": {
            "max": max(throughput_vals),
            "avg": sum(throughput_vals) / len(throughput_vals),
            "TPS": throughput_vals
        },
        "duplicate_messages": len(dup)
    }

    print(f'Done. Persisted metrics in {save_dir}/{exp_name}.json')
    with open(f'{save_dir}/{exp_name}.json', 'w', encoding='utf-8') as f:
        json.dump(res_dict, f, ensure_ascii=False, indent=4)
