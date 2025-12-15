import os

import numpy as np
import pandas as pd
import argparse

# -----------------------------
# Parse command-line arguments
# -----------------------------
parser = argparse.ArgumentParser(description="Generate Styx experiment config CSV")
parser.add_argument("--partitions", type=int, required=True, help="Number of partitions")
parser.add_argument("--n_keys", type=int, required=True, help="Number of keys")
parser.add_argument("--experiment_time", type=int, required=True, help="Total experiment time (seconds)")
parser.add_argument("--warmup_time", type=int, required=True, help="Warmup time (seconds)")
parser.add_argument(
    "--scenarios",
    nargs="+",
    default=["ycsbt_uni", "ycsbt_zipf", "dmr", "dhr", "tpcc"],
    help="Which scenarios to generate (default: all)"
)

args = parser.parse_args()
partitions = args.partitions
n_keys = args.n_keys
experiment_time = args.experiment_time
warmup_time = args.warmup_time
scenarios = set(args.scenarios)

script_path = os.path.dirname(os.path.realpath(__file__))

results_path = "results"

file_names = [f for f in os.listdir(results_path) if os.path.isfile(os.path.join(results_path, f))]

ycsbt_results = [file_name for file_name in file_names if file_name.startswith("ycsbt")]
d_movie_results = [file_name for file_name in file_names if file_name.startswith("d_movie")]
d_hotel_results = [file_name for file_name in file_names if file_name.startswith("d_hotel")]
tpcc_results = [file_name for file_name in file_names if file_name.startswith("tpcc_W")]

lines = []

# UNIFORM
zipf_const = 0.0
input_throughput = [(100, 1),
                    (200, 1),
                    (300, 1),
                    (500, 1),
                    (700, 1),
                    (1000, 1),
                    (1500, 1),
                    (2000, 1),
                    (3000, 1),
                    (3000, 2),
                    (4000, 2),
                    (5000, 2),
                    (4000, 3),
                    (5000, 3),
                    (4000, 4),
                    (5000, 4),
                    (4400, 5),
                    (4800, 5),
                    (5200, 5),
                    (5600, 5),
                    (5000, 6),
                    (5500, 6),
                    (3400, 10),
                    (3500, 10),
                    (3600, 10),
                    (3700, 10),
                    (3800, 10),
                    (3900, 10),
                    (4000, 10),
                    (4100, 10),
                    (4200, 10),
                    (4300, 10),
                    (4400, 10),
                    (4500, 10),
                    (4600, 10),
                    (4700, 10),
                    (4800, 10),
                    (4900, 10),
                    (5000, 10),
                    (5100, 10),
                    (5200, 10),
                    (5300, 10),
                    (5400, 10),
                    (5500, 10),
                    (5600, 10),
                    (5700, 10),
                    (5800, 10),
                    (5900, 10),
                    (6000, 10),
                    (10000, 10),
                    (10000, 11),
                    (10000, 12),
                    (10000, 13),
                    (10000, 14),
                    (10000, 15),
                    (6400, 25),
                    (6800, 25),
                    (7200, 25),
                    (8000, 25),
                    (8400, 25),
                    (8600, 25),
                    (9200, 25),
                    (9600, 25)
                    ]

if "ycsbt_uni" in scenarios:
    for input_rate, n_threads in input_throughput:
        file_name = f"ycsbt_uni_{input_rate * n_threads}.json"
        if file_name not in ycsbt_results:
            lines.append(('ycsbt', input_rate, n_keys, partitions, zipf_const, n_threads,
                          experiment_time, warmup_time, 1_000, True, True, True))

# ZIPF

input_throughput = [(200, 1),
                    (700, 1),
                    (1000, 1),
                    (2000, 1),
                    (3000, 1),
                    (3000, 2),
                    (3500, 2),
                    (4000, 2)]
zipf_const_list = [0.1,
                   0.2,
                   0.3,
                   0.4,
                   0.5,
                   0.6,
                   0.7,
                   0.8,
                   0.9,
                   0.99,
                   0.999]

if "ycsbt_zipf" in scenarios:
    for input_rate, n_threads in input_throughput:
        for zipf_const in zipf_const_list:
            file_name = f"ycsbt_zipf_{zipf_const}_{input_rate * n_threads}.json"
            if file_name not in ycsbt_results:
                lines.append(('ycsbt', input_rate, n_keys, partitions,
                              zipf_const, n_threads, experiment_time, warmup_time, 100, True, True, True))


# deathstar hotel reservation
input_throughput = [(100, 1),
                    (300, 1),
                    (500, 1),
                    (700, 1),
                    (1000, 1),
                    (1500, 1),
                    (2000, 1),
                    (3000, 1),
                    (3000, 2),
                    (4000, 2),
                    (5000, 2),
                    (4000, 3),
                    (5000, 3),
                    (6000, 3),
                    (7000, 3),
                    (5000, 5),
                    (6000, 5),
                    (7000, 5),
                    (8000, 5),
                    (9000, 5),
                    (10000, 5),
                    (5500, 10),
                    (6000, 10)]

if "dhr" in scenarios:
    for input_rate, n_threads in input_throughput:
        file_name = f"d_hotel_reservation_{input_rate * n_threads}.json"
        if file_name not in d_hotel_results:
            lines.append(('dhr', input_rate, -1, partitions, 0.0, n_threads, experiment_time, warmup_time, 1_000, True, True, True))


# deathstar movie review

input_throughput = [(100, 1),
                    (300, 1),
                    (500, 1),
                    (700, 1),
                    (1000, 1),
                    (1500, 1),
                    (2000, 1),
                    (3000, 1),
                    (3000, 2),
                    (4000, 2),
                    (5000, 2),
                    (4000, 3),
                    (5000, 3),
                    (6000, 3),
                    (4000, 5),
                    (6000, 4),
                    (5000, 5)]

if "dmr" in scenarios:
    for input_rate, n_threads in input_throughput:
        file_name = f"d_movie_review_{input_rate * n_threads}.json"
        if file_name not in d_movie_results:
            lines.append(('dmr', input_rate, -1, partitions, 0.0, n_threads, experiment_time, warmup_time, 1_000, True, True, True))


# tpcc
n_workers = [1, 10, 100]

# per-worker caps
max_rate = {
    1: 1000,
    10: 4000,
    100: 10000,
}


min_val = 100
max_val = 7000
num_intervals = 200

input_throughput = [
    (int(x), 1)
    for x in np.linspace(min_val, max_val, num_intervals)
]


# define the four configurations
configs = [
    # (enable_compression, use_composite_keys, use_fallback_cache, suffix)
    (True,  True,  True,  "ALL"),
    (False, True,  True,  "NO_COMP"),
    (True,  False, True,  "NO_CK"),
    (True,  True,  False, "NO_FC"),
]

if "tpcc" in scenarios:
    for input_rate, n_threads in input_throughput:
        for n_w in n_workers:
            # skip rates above the per-worker cap
            if input_rate > max_rate[n_w]:
                continue
            for enable_compression, use_composite_keys, use_fallback_cache, tag in configs:
                # file name now encodes the variant tag
                file_name = f"tpcc_W{n_w}_{input_rate * n_threads}_{tag}.json"

                if file_name not in tpcc_results:
                    lines.append((
                        'tpcc',
                        input_rate,
                        n_w,
                        partitions,
                        0.0,
                        n_threads,
                        experiment_time,
                        warmup_time,
                        100,
                        enable_compression,
                        use_composite_keys,
                        use_fallback_cache
                    ))

df = pd.DataFrame(lines)
df.to_csv(os.path.join(script_path, 'styx_experiments_config.csv'), index=False, header=False)
