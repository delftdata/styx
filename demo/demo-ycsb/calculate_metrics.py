import json
import math
import sys

import pandas as pd
import numpy as np

# A run with sensible defaults: python calculate_metrics.py results 0 10 10 100 0.0 1

def main(
        n_keys,
        n_partitions,
        input_rate,
        zipf_const,
        client_threads,
        warmup_seconds,
        SAVE_DIR,
        run_with_validation

):
    print('Calculating metrics...')

    if zipf_const > 0:
        exp_name = f"ycsbt_zipf_{zipf_const}_{input_rate * client_threads}"
    else:
        exp_name = f"ycsbt_uni_{input_rate * client_threads}"

    starting_money = 1_000_000

    res_dict = {}

    origin_input_msgs = pd.read_csv(f'{SAVE_DIR}/client_requests.csv',
                                    dtype={'request_id': bytes,
                                        'timestamp': np.uint64}).sort_values('timestamp')

    duplicate_requests = not origin_input_msgs['request_id'].is_unique


    output_msgs = pd.read_csv(f'{SAVE_DIR}/output.csv', dtype={'request_id': bytes,
                                                            'timestamp': np.uint64},
                            low_memory=False).sort_values('timestamp')
    print(
        f'Found {len(output_msgs)} output messages with {len(output_msgs['response'].dropna())} non-empty responses.'
    )

    exactly_once_output = output_msgs['request_id'].is_unique

    output_run_messages = output_msgs.iloc[n_partitions:]

    # remove warmup from input
    input_msgs = origin_input_msgs.loc[(origin_input_msgs['timestamp'] -
                                        origin_input_msgs['timestamp'][0] >= warmup_seconds * 1000)]
    print(
        f'Removed {len(origin_input_msgs) - len(input_msgs)} input messages '
        f'due to an initial warmup period of {warmup_seconds} seconds.'
    )

    joined = pd.merge(input_msgs, output_run_messages, on='request_id', how='outer')
    print(
        f'Joined {len(joined)} messages from {len(input_msgs)} input messages '
        f'and {len(output_run_messages)} output messages.'
    )

    missed = len(joined[joined['response'].isna()])
    print(
        f'Missed {missed} messages after the join for which no response '
         'was found.'
    )

    joined = joined.dropna()
    runtime = joined['timestamp_y'] - joined['timestamp_x']

    '''
    print(origin_input_msgs['timestamp'] -
                                        origin_input_msgs['timestamp'][0])
    print(warmup_seconds * 1000)
    print(origin_input_msgs['timestamp'][0])
    print(origin_input_msgs)
    print(input_msgs)
    print(joined)
    print(runtime)
    '''


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

    if run_with_validation:
        # Consistency test
        verification_state_reads = {int(e[0]): int(e[1]) for e in [res.strip('][').split(', ')
                                                                for res in output_msgs['response'].tail(n_keys)]}

        verification_total = sum(verification_state_reads.values())

        total_consistent: bool = verification_total == n_keys * starting_money
        res_dict["total_consistent"] = total_consistent
        res_dict["total_money"] = verification_total
        transaction_operations = [(int(op[0]), int(op[1]))
                                for op in [op.split(' ')[1].split('->')
                                            for op in origin_input_msgs['op']]]
        true_res = {key: starting_money for key in range(n_keys)}

        for op in transaction_operations:
            send_key, rcv_key = op
            true_res[send_key] -= 1
            true_res[rcv_key] += 1

        are_we_consistent = true_res == verification_state_reads
        res_dict["are_we_consistent"] = are_we_consistent

        missing_verification_keys = []
        wrong_values = []
        for res in true_res.items():
            key, value = res
            if key in verification_state_reads and verification_state_reads[key] != value:
                wrong_values.append(f'For key: {key}|{key % n_partitions} the value should be {value}'
                                    f' but it is {verification_state_reads[key]} | '
                                    f'{verification_state_reads[key] - value}')
            elif key not in verification_state_reads:
                missing_verification_keys.append(key)
        missing_verification_keys = len(missing_verification_keys)
        res_dict["missing_verification_keys"] = missing_verification_keys
        res_dict["wrong_values"] = wrong_values

    print(f'Done. Persisted metrics in {SAVE_DIR}/{exp_name}.json')

    with open(f'{SAVE_DIR}/{exp_name}.json', 'w', encoding='utf-8') as f:
        json.dump(res_dict, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':

    SAVE_DIR: str = sys.argv[1]
    warmup_seconds = int(sys.argv[2])
    n_keys = int(sys.argv[3])
    n_partitions = int(sys.argv[4])
    input_rate = int(sys.argv[5])
    zipf_const = float(sys.argv[6])
    client_threads = int(sys.argv[7])
    run_with_validation = bool(sys.argv[8])

    main(
        n_keys,
        n_partitions,
        input_rate,
        zipf_const,
        client_threads,
        warmup_seconds,
        SAVE_DIR,
        run_with_validation
    )