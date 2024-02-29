import os

import pandas as pd


experiment_time = 60
warmup_time = 10
script_path = os.path.dirname(os.path.realpath(__file__))

results_path = "results"

file_names = [f for f in os.listdir(results_path) if os.path.isfile(os.path.join(results_path, f))]

ycsbt_results = [file_name for file_name in file_names if file_name.startswith("ycsbt")]

input_throughput = [(5000, 1),
                    (6000, 1),
                    (7000, 1),
                    (8000, 1),
                    (4500, 2),
                    (5000, 2),
                    (4000, 3),
                    (5000, 3),
                    (4000, 4),
                    (4250, 4),
                    (4500, 4),
                    (4750, 4),
                    (5000, 4),
                    (4400, 5),
                    (4800, 5),
                    (5200, 5),
                    (5600, 5),
                    (5000, 6),
                    (3100, 10),
                    (3200, 10),
                    (5500, 6),
                    (6800, 5),
                    (7000, 5),
                    (6000, 6),
                    (3700, 10),
                    (3800, 10),
                    (3900, 10),
                    (4000, 10),
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
                    (6100, 10),
                    (6200, 10),
                    (6300, 10),
                    (6400, 10),
                    (6500, 10),
                    (10000, 10),]

perc_multy = [0.0,
              0.01,
              0.05,
              0.1,
              0.2,
              0.5,
              1.0]

parts = [2,
         4,
         6,
         8,
         10,
         12,
         14,
         16]

viable_ranges = {2: (5_000, 10_000),
                 4: (10_000, 20_000),
                 6: (20_000, 30_000),
                 8: (30_000, 40_000),
                 10: (35_000, 45_000),
                 12: (45_000, 55_000),
                 14: (50_000, 60_000),
                 16: (55_000, 65_000)}

lines = []
for input_rate, n_threads in input_throughput:
    total_rate = input_rate * n_threads
    for part in parts:
        if viable_ranges[part][0] <= total_rate <= viable_ranges[part][1]:
            for pm in perc_multy:
                file_name = f"ycsbt_scale_{part}_{pm}_{total_rate}.json"
                if file_name not in ycsbt_results:
                    lines.append((input_rate, part, pm,  n_threads, experiment_time, warmup_time, 1000))

df = pd.DataFrame(lines)
df.to_csv(os.path.join(script_path, 'styx_scalability_experiments_config.csv'), index=False, header=False)
