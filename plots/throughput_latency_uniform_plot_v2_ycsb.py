import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams

rcParams['figure.figsize'] = [12, 5]
plt.rcParams.update({'font.size': 18})
x = np.array([100, 200, 300, 400, 500, 700, 1000, 1200, 1300, 1500, 1700, 2000, 3000, 6000, 8000, 10000, 12000, 15000,
              16000, 20000, 22000, 24000, 26000, 28000, 30000, 33000, 34000, 40000, 44000, 48000, 49000, 50000, 52000, 55000])
y_styx_50 = np.array([15, 15, 14, None, 18, 18, 18, None, None, 18, None, 19, 19, 22, 22,
                      23, 24, 25, 25, 25, 26, 27, 29, 28, 32, 39, 43, 72, 119, 308, 502, 1382, 2842, 7514]).astype(np.double)
styx_50_mask = np.isfinite(y_styx_50)
y_beldi_50 = [147.15, 166.03, 192.56, 6958.25, 16990.24] + [None] * (len(x) - 5)
y_statefun_50 = np.array([224, None, 213, None, 181, 148, 158, 124, None, 149, None, 695, 14721]
                         + [None] * (len(x) - 13)).astype(np.double)
y_statefun_50_mask = np.isfinite(y_statefun_50)
y_boki_50 = np.array([32.88, None, 35.21, None, 33.38, 33.91, 38.05, 43.94, 659.74, 3724.51, 8447.45]
                     + [None] * (len(x) - 11)).astype(np.double)
y_boki_50_mask = np.isfinite(y_boki_50)
y_styx_99 = np.array([20, 20, 20, None, 29, 29, 30, None, None, 32, None, 34, 35, 42, 41, 42, 43, 51, 48, 50, 57,
                      65, 97, 99, 108, 128, 152, 199, 280, 665, 1733, 1827, 3433, 11647]).astype(np.double)
styx_99_mask = np.isfinite(y_styx_99)
y_beldi_99 = [234.85, 2543.85, 6172.14, 15276.74, 30519.72] + [None] * (len(x) - 5)
y_statefun_99 = np.array([348, None, 347, None, 371, 303, 358, 378, None, 428, None, 2280, 26306]
                         + [None] * (len(x) - 13)).astype(np.double)
y_statefun_99_mask = np.isfinite(y_statefun_99)
y_boki_99 = np.array([47.25, None, 53.22, None, 47.9, 58.04, 3478.05, 5656.14, 6589.18, 6857.06, 10316.61]
                     + [None] * (len(x) - 11)).astype(np.double)
y_boki_99_mask = np.isfinite(y_boki_99)

line_width = 2.5
marker_size = 8
plt.grid(linestyle="--", linewidth=1.5, which='major', axis='both')
plt.plot(x[styx_50_mask], y_styx_50[styx_50_mask], "-o", color="#882255", label="Styx 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[styx_99_mask], y_styx_99[styx_99_mask], "--", marker="o", color="#882255", label="Styx 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor='none')
plt.plot(x, y_beldi_50, "-^", color="#BD7105", label="Beldi 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x, y_beldi_99, "--", marker="^", color="#BD7105", label="Beldi 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor='none')
plt.plot(x[y_statefun_50_mask], y_statefun_50[y_statefun_50_mask], "-P", color="#005F20", label="T-Statefun 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[y_statefun_99_mask], y_statefun_99[y_statefun_99_mask], "--", marker="P", color="#005F20",
         label="T-Statefun 99p", linewidth=line_width, markersize=marker_size, markerfacecolor='none')
plt.plot(x[y_boki_50_mask], y_boki_50[y_boki_50_mask], "-d", color="#332288", label="Boki 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[y_boki_99_mask], y_boki_99[y_boki_99_mask], "--", marker="d", color="#332288", label="Boki 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor='none')
plt.legend(bbox_to_anchor=(0.5, 1.2), loc="center", ncol=4)
# plt.ylim([1, 100000])
# plt.xlim([100, 50000])
plt.ylabel("Latency (ms)")
plt.xlabel("Input Throughput (transactions/s)")
plt.yscale('log', base=10)
plt.xscale('log', base=10)
plt.tight_layout()
plt.savefig("throughput_latency_uniform.pdf")
plt.show()
