from matplotlib import rcParams
import matplotlib.pyplot as plt
import numpy as np

sys_name = "SysX"

rcParams["figure.figsize"] = [12, 5]
plt.rcParams.update({"font.size": 18})

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
plt.savefig("throughput_latency_deathstar_flight.pdf")
plt.show()
