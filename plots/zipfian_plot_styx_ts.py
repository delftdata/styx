import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams

rcParams['figure.figsize'] = [12, 5]
plt.rcParams.update({'font.size': 18})

x_labels = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99, 0.999]

x = np.array(x_labels)
y_styx_50 = [19, 28, 28, 29, 29, 28, 28, 28, 30, 30, 30, 30]
y_styx_99 = [35, 58, 53, 57, 55, 54, 53, 55, 58, 57, 58, 57]
y_styx3k_50 = [19, 30, 29, 30, 30, 28, 29, 29, 30, 29, 34738, 39676]
y_styx3k_99 = [35, 63, 62, 62, 63, 57, 58, 59, 60, 59, 61637, 69093]
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
plt.plot(x, y_styx_50, "-^", color="#B65FCF", label="Styx@2K 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x, y_styx_99, "--", marker="^", color="#B65FCF", label="Styx@2K 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor='none')
plt.plot(x, y_styx3k_50, "-o", color="#882255", label="Styx@3K 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x, y_styx3k_99, "--", marker="o", color="#882255", label="Styx@3K 99p",
         linewidth=line_width, markersize=marker_size, markerfacecolor='none')
plt.plot(x[statefun_50_mask], y_statefun_50[statefun_50_mask], "-P", color="#005F20", label="T-Statefun@700 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[statefun_99_mask], y_statefun_99[statefun_99_mask], "--", marker="P", color="#005F20",
         label="T-Statefun@700 99p", linewidth=line_width, markersize=marker_size, markerfacecolor='none')

# plt.ylim([0, 3])
plt.ylabel("Latency (ms)")
plt.xlabel("Zipfian const")
plt.legend(bbox_to_anchor=(0.5, 1.2), loc="center", ncol=3, prop={'size': 21})
plt.yscale('log', base=10)
plt.ylim([10, 9000])
plt.tight_layout()
plt.savefig("zipfian.pdf")
plt.show()
