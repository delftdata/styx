import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, rc

rcParams['figure.figsize'] = [12, 5]
plt.rcParams.update({'font.size': 18})

x = np.array([100,
              300,
              500,
              700,
              1000,
              1500,
              2000,
              2400,
              3000,
              3500])

# x = x * 60
labels = [f"{int(x)}K" for x in (x / 1000) if x not in [18, 42]]

y_w1_50 = np.array([28, 30, 67587] + [None] * (len(x) - 3)).astype(np.double)
y_w1_99 = np.array([50, 54, 81581] + [None] * (len(x) - 3)).astype(np.double)
w1_50_mask = np.isfinite(y_w1_50)
w1_99_mask = np.isfinite(y_w1_99)

y_w10_50 = np.array([28, 29, 34, 31, 31, 36, 335, 42483] + [None] * (len(x) - 8)).astype(np.double)
y_w10_99 = np.array([72, 86, 113, 85, 58, 88, 787, 88715] + [None] * (len(x) - 8)).astype(np.double)
w10_50_mask = np.isfinite(y_w10_50)
w10_99_mask = np.isfinite(y_w10_99)

y_w100_50 = np.array([28, 30, 31, 31, 30, 34, 40, 245, 508, 20345] + [None] * (len(x) - 10)).astype(np.double)
y_w100_99 = np.array([56, 107, 104, 112, 63, 121, 169, 571, 923, 39117] + [None] * (len(x) - 10)).astype(np.double)
w100_50_mask = np.isfinite(y_w100_50)
w100_99_mask = np.isfinite(y_w100_99)


line_width = 2.5
marker_size = 8
plt.grid(linestyle="--", linewidth=1.5)
plt.plot(x[w1_50_mask], y_w1_50[w1_50_mask], "-o", color="#4B0082", label="W=1 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[w1_99_mask], y_w1_99[w1_99_mask], "--", marker="o", color="#4B0082", label="W=1 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor='none')
plt.plot(x[w10_50_mask], y_w10_50[w10_50_mask], "-^", color="#8B008B", label="W=10 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[w10_99_mask], y_w10_99[w10_99_mask], "--", marker="^", color="#8B008B", label="W=10 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor='none')
plt.plot(x[w100_50_mask], y_w100_50[w100_50_mask], "-d", color="#B65FCF", label="W=100 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[w100_99_mask], y_w100_99[w100_99_mask], "--", marker="d", color="#B65FCF", label="W=100 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor='none')
plt.legend(bbox_to_anchor=(0.5, 1.2), loc="center", ncol=3)
# plt.ylim([1, 100000])
# plt.xlim([100, 50000])
plt.ylabel("Latency (ms)")
plt.xlabel("Input Throughput (transactions/s)")
plt.yscale('log', base=10)
# plt.xticks([i for i in x if i not in [18000, 42000]],
#            labels)
# plt.xscale('log', base=10)
plt.tight_layout()
plt.savefig("throughput_latency_tpcc.pdf")
plt.show()
