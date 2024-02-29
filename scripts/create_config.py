import os

import pandas as pd

partitions = 96
n_keys = 10_000
experiment_time = 60
warmup_time = 10
script_path = os.path.dirname(os.path.realpath(__file__))

results_path = "results"

file_names = [f for f in os.listdir(results_path) if os.path.isfile(os.path.join(results_path, f))]

ycsbt_results = [file_name for file_name in file_names if file_name.startswith("ycsbt")]
d_movie_results = [file_name for file_name in file_names if file_name.startswith("d_movie")]
d_hotel_results = [file_name for file_name in file_names if file_name.startswith("d_hotel")]
tpcc_results = [file_name for file_name in file_names if file_name.startswith("tpcc_W")]

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
                    (10000, 15)
                    ]

lines = []
for input_rate, n_threads in input_throughput:
    file_name = f"ycsbt_uni_{input_rate * n_threads}.json"
    if file_name not in ycsbt_results:
        lines.append(('ycsbt', input_rate, n_keys, partitions, zipf_const, n_threads,
                      experiment_time, warmup_time, 1_000))

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

for input_rate, n_threads in input_throughput:
    for zipf_const in zipf_const_list:
        file_name = f"ycsbt_zipf_{zipf_const}_{input_rate * n_threads}.json"
        if file_name not in ycsbt_results:
            lines.append(('ycsbt', input_rate, n_keys, partitions,
                          zipf_const, n_threads, experiment_time, warmup_time, 100))


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

for input_rate, n_threads in input_throughput:
    file_name = f"d_hotel_reservation_{input_rate * n_threads}.json"
    if file_name not in d_hotel_results:
        lines.append(('dhr', input_rate, -1, partitions, 0.0, n_threads, experiment_time, warmup_time, 1_000))


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

for input_rate, n_threads in input_throughput:
    file_name = f"d_movie_review_{input_rate * n_threads}.json"
    if file_name not in d_movie_results:
        lines.append(('dmr', input_rate, -1, partitions, 0.0, n_threads, experiment_time, warmup_time, 1_000))


# tpcc

n_workers = [1, 10, 100, 200]
input_throughput = [(100, 1),
                    (200, 1),
                    (300, 1),
                    (400, 1),
                    (500, 1),
                    (600, 1),
                    (700, 1),
                    (800, 1),
                    (900, 1),
                    (1000, 1),
                    (1100, 1),
                    (1400, 1),
                    (1500, 1),
                    (1600, 1),
                    (2000, 1),
                    (2400, 1),
                    (3000, 1),
                    (3500, 1)]

for input_rate, n_threads in input_throughput:
    for n_w in n_workers:
        file_name = f"tpcc_W{n_w}_{input_rate * n_threads}.json"
        if file_name not in tpcc_results:
            lines.append(('tpcc', input_rate, n_w, partitions, 0.0,
                          n_threads, experiment_time, warmup_time, 100))

df = pd.DataFrame(lines)
df.to_csv(os.path.join(script_path, 'styx_experiments_config.csv'), index=False, header=False)
