from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, rc

rcParams['figure.figsize'] = [12, 5]
plt.rcParams.update({'font.size': 18})


def parse_log_file(f_name: str):
    avg_sn_time: dict[int, float] = {}
    prev_id = 0
    cache = []
    with open(f_name, "r") as f:
        while True:
            line = f.readline()
            if not line:
                break
            parts = line.split("|")
            if len(parts) > 3:
                sn_id: int = int(parts[2].strip().split(" ")[-1])
                time_ms = float(parts[5].strip().split(" ")[-1][0:-2])
                if sn_id != prev_id:
                    avg_sn_time[sn_id] = sum(cache) / len(cache)
                    prev_id = sn_id
                    cache = []
                else:
                    cache.append(time_ms)
    return avg_sn_time


"""
Populating Warehouse: 200it [00:00, 112177.16it/s]
Populating District: 2000it [00:00, 98911.77it/s]
Populating Customer: 6000000it [01:17, 77517.72it/s]
Populating History: 6000000it [00:33, 179164.80it/s]
Populating New Order: 1800000it [00:07, 226798.34it/s]
Populating Order: 6000000it [00:31, 189488.40it/s]
Populating Order Line: 60003752it [06:20, 157668.49it/s]
Populating Item: 100000it [00:00, 315199.57it/s]
Populating Stock: 20000000it [02:54, 114894.10it/s]
Data populated waiting for 1 minute
"""

lat_per_sn = parse_log_file("incrementality_logs.txt")

print(lat_per_sn)

x = [sn_id * 10 for sn_id in lat_per_sn.keys()]
latencies = [lat for lat in lat_per_sn.values()]

init_time = 77 + 33 + 7 + 31 + 380 + 174
start_load_time = init_time + 60

line_width = 2.5
marker_size = 8
plt.grid(linestyle="--", linewidth=1.5)
plt.plot(x, latencies, "-", color="#882255",
         linewidth=line_width, markersize=marker_size)
plt.axvline(x=10, color='#005F20', label='Start bulk load ~ 20GB', linewidth=line_width, linestyle="--")
plt.axvline(x=start_load_time, color='#332288', label='Start TPC-C@ 1K TPS', linewidth=line_width, linestyle="--")
plt.legend(loc="upper left")
# plt.ylim([1, 1000])
# plt.xlim([start_load_time, x[-1]])
plt.ylabel("Snapshotting time (ms)")
plt.xlabel("Runtime (s)")
plt.yscale('log', base=10)
# plt.xscale('log', base=10)
plt.tight_layout()
plt.savefig("incremental_snapshots.pdf")
plt.show()
