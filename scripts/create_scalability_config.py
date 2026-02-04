import os

import pandas as pd

experiment_time = 60
warmup_time = 10
script_path = os.path.dirname(os.path.realpath(__file__))

results_path = "../s_results/s_results"

file_names = [
    f for f in os.listdir(results_path)
    if os.path.isfile(os.path.join(results_path, f))
]
ycsbt_results = [file_name for file_name in file_names
                 if file_name.startswith("ycsbt")]

# ============================================================
#                  INPUT THROUGHPUT GENERATION
# ============================================================

# Workers we want to test
workers = [2, 4, 6, 8, 10, 12, 14, 16, 24, 32, 64]
multipartitions = [0.0, 0.2, 0.5, 1.0]

input_throughput = []

for w in workers:
    min_total = 4000 * w
    max_total = 10000 * w

    # rates from 4000 to 10000 inclusive, step 250
    for rate in range(4000, 10001, 250):
        total = rate * w
        if min_total <= total <= max_total:
            input_throughput.append((rate, w))

# Remove duplicates by total throughput (keep first occurrence)
new_input_throughput = []
total_seen = set()

for rate, w in input_throughput:
    total = rate * w
    if total not in total_seen:
        total_seen.add(total)
        new_input_throughput.append((rate, w))

input_throughput = new_input_throughput

per_worker_target = {
    0.0: 6200,
    0.2: 4900,
    0.5: 4100,
    1.0: 3200,
}

band = 0.10               # ±10% band around target
shift = 1.21              # 10% shift twice → +21%

viable_ranges = {}

for w in workers:
    min_total = 4000 * w
    max_total = 10000 * w

    for pm in multipartitions:
        base = per_worker_target[pm] * w

        # base band:
        raw_r1 = base * (1.0 - band)
        raw_r2 = base * (1.0 + band)

        # shift everything to the right again (now total shift is +21%)
        raw_r1 *= shift
        raw_r2 *= shift

        # clamp to system limits
        r1 = int(max(raw_r1, min_total))
        r2 = int(min(raw_r2, max_total))

        # ensure valid interval even for small worker counts
        if r1 > r2:
            r1 = min_total
            r2 = int(min(max_total, min_total * (1.0 + 2 * band)))

        viable_ranges[(w, pm)] = (r1, r2)

# ============================================================
#                  MATCH TO EXPERIMENTS
# ============================================================

lines = []

for input_rate, n_threads in input_throughput:
    total_rate = input_rate * n_threads
    for (part_workers, pm), (r1, r2) in viable_ranges.items():
        if r1 <= total_rate <= r2:
            file_name = f"ycsbt_scale_{part_workers}_{pm}_{total_rate}.json"
            if file_name not in ycsbt_results:
                # fields: rate, #workers, pm, client-threads,
                #         experiment_time, warmup_time, 1000 (keys/whatever)
                lines.append(
                    (input_rate, part_workers, pm,
                     n_threads, experiment_time, warmup_time, 1000)
                )

df = pd.DataFrame(lines)
df.to_csv(
    os.path.join(script_path, "styx_scalability_experiments_config.csv"),
    index=False,
    header=False,
)
