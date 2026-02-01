from matplotlib import rcParams
from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import numpy as np

rcParams["figure.figsize"] = [24, 8]
plt.rcParams.update({"font.size": 22})

line_width = 2.5
marker_size = 8
styx_color = "#882255"
beldi_color = "#BD7105"
boki_color = "#332288"
tstatefun_color = "#005F20"

fig, ax = plt.subplots(1, 3)

# YCSB-T

x = np.array([100, 200, 300, 400, 500, 700, 1000, 1200, 1300, 1500, 1700, 2000, 3000, 6000, 8000, 10000, 12000, 15000,
              16000, 20000, 22000, 24000, 26000, 28000, 30000, 33000, 34000, 40000, 44000, 48000,
              49000, 50000, 52000, 55000])

y_styx_50 = np.array([15, 15, 14, None, 18, 18, 18, None, None, 18, None, 19, 19, 22, 22,
                      23, 24, 25, 25, 25, 26, 27, 29, 28, 32, 39, 43, 72, 119, 308, 502,
                      1382, 2842, 7514]).astype(np.double)
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

ax[0].grid(linestyle="--", linewidth=1.5, which="major", axis="both")
ax[0].plot(x[styx_50_mask], y_styx_50[styx_50_mask], "-o", color="#882255",
           linewidth=line_width, markersize=marker_size)
ax[0].plot(x[styx_99_mask], y_styx_99[styx_99_mask], "--", marker="o", color="#882255",
           linewidth=line_width, markersize=marker_size, markerfacecolor="none")
ax[0].plot(x, y_beldi_50, "-^", color="#BD7105",
           linewidth=line_width, markersize=marker_size)
ax[0].plot(x, y_beldi_99, "--", marker="^", color="#BD7105",
           linewidth=line_width, markersize=marker_size, markerfacecolor="none")
ax[0].plot(x[y_statefun_50_mask], y_statefun_50[y_statefun_50_mask], "-P", color="#005F20",
           linewidth=line_width, markersize=marker_size)
ax[0].plot(x[y_statefun_99_mask], y_statefun_99[y_statefun_99_mask], "--", marker="P", color="#005F20",
           linewidth=line_width, markersize=marker_size, markerfacecolor="none")
ax[0].plot(x[y_boki_50_mask], y_boki_50[y_boki_50_mask], "-d", color="#332288",
           linewidth=line_width, markersize=marker_size)
ax[0].plot(x[y_boki_99_mask], y_boki_99[y_boki_99_mask], "--", marker="d", color="#332288",
           linewidth=line_width, markersize=marker_size, markerfacecolor="none")
ax[0].set_yscale("log", base=10)
ax[0].set_xscale("log", base=10)
ax[0].set_xlabel("Input Throughput (transactions/s)")
ax[0].set_ylabel("Latency (ms)")
ax[0].title.set_text("YCSB-T")

# Deathstar travel

x = np.array([100, 300, 500, 700, 1000, 1500, 2000, 3000, 4000, 5000,
              6000, 8000, 10000, 12000, 15000, 18000, 21000, 25000, 30000,
              35000, 40000, 45000, 50000, 55000, 60000, 65000, 100000])

y_styx_50 = np.array([11, 9, 8, 9, 11, 12, 15, 29, None, None,
                      39, 37, 42, 45, 53, 58, 59, 62, 70, 262, 342, 473, 432, 457, 586, 618, 840]).astype(np.double)
styx_50_mask = np.isfinite(y_styx_50)

y_beldi_50 = [70.8, 77.82, 91.69, 6076.13] + [None] * (len(x) - 4)

y_boki_50 = np.array([18.02, 18.11, 17.59,
                      19.44,
                      19.27,
                      18.36,
                      20.19,
                      20.72,
                      5288.6,
                      11167.56] + [None] * (len(x) - 10)).astype(np.double)
y_boki_50_mask = np.isfinite(y_boki_50)

y_styx_99 = np.array([17,
                      18,
                      18,
                      18,
                      21,
                      29,
                      57.0,
                      128, None, None, 157, 471, 162,
                      220, 557, 658, 563, 542, 256, 768, 802, 848, 865, 895, 962, 1008, 1412]).astype(np.double)
styx_99_mask = np.isfinite(y_styx_99)

y_beldi_99 = [354.52, 7633.99, 12789.47, 20674.15] + [None] * (len(x) - 4)

y_boki_99 = np.array([28.83,
                      28.85,
                      28.9,
                      48.83,
                      32.98,
                      34.22,
                      1635.99,
                      1574.08,
                      11526.96,
                      23038.02] + [None] * (len(x) - 10)).astype(np.double)
y_boki_99_mask = np.isfinite(y_boki_99)

ax[1].grid(linestyle="--", linewidth=1.5)
ax[1].plot(x[styx_50_mask], y_styx_50[styx_50_mask], "-o", color="#882255",
           linewidth=line_width, markersize=marker_size)
ax[1].plot(x[styx_99_mask], y_styx_99[styx_99_mask], "--", marker="o", color="#882255",
           linewidth=line_width, markersize=marker_size, markerfacecolor="none")
ax[1].plot(x, y_beldi_50, "-^", color="#BD7105",
           linewidth=line_width, markersize=marker_size)
ax[1].plot(x, y_beldi_99, "--", marker="^", color="#BD7105",
           linewidth=line_width, markersize=marker_size, markerfacecolor="none")
ax[1].plot(x[y_boki_50_mask], y_boki_50[y_boki_50_mask], "-d", color="#332288",
           linewidth=line_width, markersize=marker_size)
ax[1].plot(x[y_boki_99_mask], y_boki_99[y_boki_99_mask], "--", marker="d", color="#332288",
           linewidth=line_width, markersize=marker_size, markerfacecolor="none")
ax[1].set_yscale("log", base=10)
ax[1].set_xscale("log", base=10)
ax[1].title.set_text("Deathstar Travel")
ax[1].set_xlabel("Input Throughput (transactions/s)")
ax[1].set_ylabel("Latency (ms)")

# Deathstar movie

x = np.array([100, 200, 300, 500, 700, 1000, 1500, 2000, 3000, 6000, 8000, 10000, 20000, 25000, 30000])

y_styx_50 = np.array([11, None, 10, 9, 10, 27, 27, 36, 27, 35, 42, 415, 624, 663, 59954]).astype(np.double)
styx_50_mask = np.isfinite(y_styx_50)

y_beldi_50 = [94.59, 95.89, 13806.32] + [None] * (len(x) - 3)

y_boki_50 = np.array([25.27, None, 26.41, 27.71, 28.99, 31.71,
                      807.89, 18701.14] + [None] * (len(x) - 8)).astype(np.double)
y_boki_50_mask = np.isfinite(y_boki_50)

y_styx_99 = np.array([18, None, 18, 19, 20, 49, 50, 50, 45, 69, 92, 796, 987, 1083, 99216]).astype(np.double)
styx_99_mask = np.isfinite(y_styx_99)

y_beldi_99 = [149.27, 7254.41, 21681.66] + [None] * (len(x) - 3)

y_boki_99 = np.array([42.31, None, 41.23, 3222,
                      5793.33, 6311.55, 9375.48, 32081.18] + [None] * (len(x) - 8)).astype(np.double)
y_boki_99_mask = np.isfinite(y_boki_99)

ax[2].grid(linestyle="--", linewidth=1.5)
ax[2].plot(x[styx_50_mask], y_styx_50[styx_50_mask], "-o", color="#882255",
           linewidth=line_width, markersize=marker_size)
ax[2].plot(x[styx_99_mask], y_styx_99[styx_99_mask], "--", marker="o", color="#882255",
           linewidth=line_width, markersize=marker_size, markerfacecolor="none")
ax[2].plot(x, y_beldi_50, "-^", color="#BD7105",
           linewidth=line_width, markersize=marker_size)
ax[2].plot(x, y_beldi_99, "--", marker="^", color="#BD7105",
           linewidth=line_width, markersize=marker_size, markerfacecolor="none")
ax[2].plot(x[y_boki_50_mask], y_boki_50[y_boki_50_mask], "-d", color="#332288",
           linewidth=line_width, markersize=marker_size)
ax[2].plot(x[y_boki_99_mask], y_boki_99[y_boki_99_mask], "--", marker="d", color="#332288",
           linewidth=line_width, markersize=marker_size, markerfacecolor="none")
ax[2].set_yscale("log", base=10)
ax[2].set_xscale("log", base=10)
ax[2].title.set_text("Deathstar Movie")
ax[2].set_xlabel("Input Throughput (transactions/s)")
ax[2].set_ylabel("Latency (ms)")

# legend
leg1 = Line2D([0], [0], color=styx_color, linewidth=line_width, linestyle="-",
              label="Styx 50p", marker="o", markersize=marker_size)
leg2 = Line2D([0], [0], color=styx_color, linewidth=line_width, linestyle="--",
              label="Styx 99p", marker="o", markersize=marker_size, markerfacecolor="none")
leg3 = Line2D([0], [0], color=tstatefun_color, linewidth=line_width, linestyle="-",
              label="T-Statefun 50p", marker="P", markersize=marker_size)
leg4 = Line2D([0], [0], color=tstatefun_color, linewidth=line_width, linestyle="--",
              label="T-Statefun 99p", marker="P", markersize=marker_size, markerfacecolor="none")
leg5 = Line2D([0], [0], color=boki_color, linewidth=line_width, linestyle="-",
              label="Boki 50p", marker="d", markersize=marker_size)
leg6 = Line2D([0], [0], color=boki_color, linewidth=line_width, linestyle="--",
              label="Boki 99p", marker="d", markersize=marker_size, markerfacecolor="none")
leg7 = Line2D([0], [0], color=beldi_color, linewidth=line_width, linestyle="-",
              label="Beldi 50p", marker="^", markersize=marker_size)
leg8 = Line2D([0], [0], color=beldi_color, linewidth=line_width, linestyle="--",
              label="Beldi 99p", marker="^", markersize=marker_size, markerfacecolor="none")
fig.legend(handles=[leg1, leg2, leg3, leg4,
                    leg5, leg6, leg7, leg8],
           bbox_to_anchor=(0.5, 0.1), loc="center", ncol=4)

# plt.tight_layout()
plt.savefig("throughput_latency_all.pdf")
plt.show()
