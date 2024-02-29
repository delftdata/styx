import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, rc

rcParams['figure.figsize'] = [14, 8]
plt.rcParams.update({'font.size': 22})

x_labels = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99, 0.999]

# x = np.arange(len(x_labels))
x = np.array(x_labels)
y_styx_50 = [19, 28, 28, 29, 29, 28, 28, 28, 30, 30, 30, 30]
y_styx_99 = [34, 58, 53, 57, 55, 54, 53, 55, 58, 57, 58, 57]
y_beldi_50 = np.array([166.03,
                       169.26,
                       179.44,
                       165.73,
                       164.87,
                       165.84,
                       167.56,
                       171.92,
                       161.99,
                       169.53,
                       173.77,
                       165.19]).astype(np.double)
y_boki_50 = np.array([33.91,
                      34.29,
                      33.94,
                      34.61,
                      35.73,
                      33.73,
                      35.27,
                      35.34,
                      34,
                      35.56,
                      34.82,
                      34.58]).astype(np.double)
beldi_50_mask = np.isfinite(y_beldi_50)
boki_50_mask = np.isfinite(y_boki_50)
y_statefun_50 = np.array([148,
                          167,
                          169,
                          167,
                          165,
                          169,
                          170,
                          161,
                          728, 1000000, 1000000, 1000000]).astype(np.double)
statefun_50_mask = np.isfinite(y_statefun_50)
y_beldi_99 = np.array([2543.85,
                       2009.48,
                       2182.68,
                       2478.91,
                       3443.83,
                       None,
                       #313.44,
                       2386.21,
                       2920.89,
                       1295.36,
                       2583.91,
                       None,
                       2920.89
                       # 275.13,
                       # 275.64
                       ]).astype(np.double)
y_boki_99 = np.array([58.04,
                      47.37,
                      1255.39,
                      3927.9,
                      None,
                      # 55.07,
                      1624.96,
                      4012.45,
                      3504.44,
                      2414.52,
                      1625.19,
                      1502.49,
                      2414.45
                      # 51.33
                      ]).astype(np.double)
beldi_99_mask = np.isfinite(y_beldi_99)
boki_99_mask = np.isfinite(y_boki_99)
y_statefun_99 = np.array([303,
                          364,
                          403,
                          382,
                          386,
                          382,
                          380,
                          412,
                          1434, 1000000, 1000000, 1000000]).astype(np.double)
statefun_99_mask = np.isfinite(y_statefun_99)

line_width = 2.5
marker_size = 8
plt.grid(linestyle="--", linewidth=1.5)
plt.plot(x, y_styx_50, "-o", color="#882255", label="Styx@2K 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x, y_styx_99, "--", marker="o", color="#882255", label="Styx@2K 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor='none')
plt.plot(x[beldi_50_mask], y_beldi_50[beldi_50_mask], "-^", color="#BD7105", label="Beldi 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[beldi_99_mask], y_beldi_99[beldi_99_mask], "--", marker="^", color="#BD7105", label="Beldi 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor='none')
plt.plot(x[statefun_50_mask], y_statefun_50[statefun_50_mask], "-P", color="#005F20", label="T-Statefun@700 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[statefun_99_mask], y_statefun_99[statefun_99_mask], "--", marker="P", color="#005F20",
         label="T-Statefun@700 99p", linewidth=line_width, markersize=marker_size, markerfacecolor='none')
plt.plot(x[boki_50_mask], y_boki_50[boki_50_mask], "-d", color="#332288", label="Boki 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[boki_99_mask], y_boki_99[boki_99_mask], "--", marker="d", color="#332288", label="Boki 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor='none')

# plt.ylim([0, 3])
plt.ylabel("Latency (ms)")
plt.xlabel("Zipfian const")
plt.legend(bbox_to_anchor=(0.5, 1.2), loc="center", ncol=4, prop={'size': 21})
plt.yscale('log', base=10)
plt.ylim([10, 10000])
plt.tight_layout()
plt.savefig("zipfian.pdf")
plt.show()
