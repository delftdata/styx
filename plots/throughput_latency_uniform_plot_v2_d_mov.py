from matplotlib import rcParams
import matplotlib.pyplot as plt
import numpy as np

sys_name = "SysX"

rcParams["figure.figsize"] = [12, 5]
plt.rcParams.update({"font.size": 18})


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

line_width = 2.5
marker_size = 8
plt.grid(linestyle="--", linewidth=1.5)
plt.plot(x[styx_50_mask], y_styx_50[styx_50_mask], "-o", color="#882255", label=f"{sys_name} 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[styx_99_mask], y_styx_99[styx_99_mask], "--", marker="o", color="#882255", label=f"{sys_name} 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor="none")
plt.plot(x, y_beldi_50, "-^", color="#BD7105", label="Beldi 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x, y_beldi_99, "--", marker="^", color="#BD7105", label="Beldi 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor="none")
plt.plot(x[y_boki_50_mask], y_boki_50[y_boki_50_mask], "-d", color="#332288", label="Boki 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[y_boki_99_mask], y_boki_99[y_boki_99_mask], "--", marker="d", color="#332288", label="Boki 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor="none")
plt.legend(bbox_to_anchor=(0.5, 1.2), loc="center", ncol=3)
# plt.ylim([1, 100000])
# plt.xlim([100, 50000])
plt.ylabel("Latency (ms)")
plt.xlabel("Input Throughput (transactions/s)")
plt.yscale("log", base=10)
plt.xscale("log", base=10)
plt.tight_layout()
plt.savefig("throughput_latency_deathstar_movie.pdf")
plt.show()
