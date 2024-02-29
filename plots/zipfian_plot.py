import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, rc

rcParams['figure.figsize'] = [10, 9]
plt.rcParams.update({'font.size': 14})

x_labels = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99, 0.999]

# x = np.arange(len(x_labels))
x = np.array(x_labels)
y_styx_50 = [25, 26, 26, 26, 25, 26, 26, 26, 26, 26, 26, 26]
y_styx_4000_50 = [25, 28, 34, 27, 27, 31, 28, 29, 31, 56, 150, 198]
y_beldi_50 = np.array([220.37, None, None, 70.56, None, 100.16, None, 66.44, None, None, 66.69, None]).astype(np.double)
beldi_50_mask = np.isfinite(y_beldi_50)
y_statefun_50 = np.array([100, None, None, None, None, None, None, None, None, None, None, None]).astype(np.double)
statefun_50_mask = np.isfinite(y_statefun_50)

y_styx_99 = [44, 50, 45, 50, 44, 51, 45, 51, 47, 49, 54, 55]
y_styx_4000_99 = [47, 64, 68, 50, 53, 97, 62, 64, 58, 300, 3355, 12812]
y_beldi_99 = np.array([405.97, None, None, 342.99, None, 388.98, None, 284.40, None, None, 6929.77, None]).astype(np.double)
beldi_99_mask = np.isfinite(y_beldi_99)
y_statefun_99 = np.array([120, None, None, None, None, None, None, None, None, None, None, None]).astype(np.double)
statefun_99_mask = np.isfinite(y_statefun_99)

fig, (ax1, ax0) = plt.subplots(2, 1)
ax0.grid(axis="y", linestyle="--")
ax0.plot(x, y_styx_50, "-o", color="#31688e", label="Styx 50p")
ax0.plot(x, y_styx_99, "--", marker="o", color="#31688e", label="Styx 99p")
# ax0.set_xticklabels(x_labels)
ax0.plot(x, y_styx_4000_50, "-o", color="#440154", label="Styx@4K 50p")
ax0.plot(x, y_styx_4000_99, "--", marker="o", color="#440154", label="Styx@4K 99p")
ax0.plot(x[beldi_50_mask], y_beldi_50[beldi_50_mask], "-^", color="#35b779", label="Beldi 50p")
ax0.plot(x[beldi_99_mask], y_beldi_99[beldi_99_mask], "--", marker="^", color="#35b779", label="Beldi 99p")
ax0.plot(x[statefun_50_mask], y_statefun_50[statefun_50_mask], "-x", color="#fde725", label="T-Statefun 50p")
ax0.plot(x[statefun_99_mask], y_statefun_99[statefun_99_mask], "--", marker="x", color="#fde725", label="T-Statefun 99p")

# ax0.legend()
# ax0.set_xlim([0, 3])
ax0.set_ylim([0, 1000])
# ax0.ylabel("Latency (ms)")
# ax0.xlabel("Input Throughput (transactions/s)")

ax1.grid(axis="y", linestyle="--")
ax1.plot(x, y_styx_50, "-o", color="#31688e", label="Styx 50p")
ax1.plot(x, y_styx_99, "--", marker="o", color="#31688e", label="Styx 99p")
ax1.plot(x, y_styx_4000_50, "-o", color="#440154", label="Styx@4K 50p")
ax1.plot(x, y_styx_4000_99, "--", marker="o", color="#440154", label="Styx@4K 99p")
ax1.plot(x[beldi_50_mask], y_beldi_50[beldi_50_mask], "-^", color="#35b779", label="Beldi 50p")
ax1.plot(x[beldi_99_mask], y_beldi_99[beldi_99_mask], "--", marker="^", color="#35b779", label="Beldi 99p")
ax1.plot(x[statefun_50_mask], y_statefun_50[statefun_50_mask], "-x", color="#fde725", label="T-Statefun 50p")
ax1.plot(x[statefun_99_mask], y_statefun_99[statefun_99_mask], "--", marker="x", color="#fde725", label="T-Statefun 99p")
ax1.legend(bbox_to_anchor=(0.5, 1.2), loc="center", ncol=4)
# ax1.set_xticklabels(x_labels)
# ax0.set_xlim([0, 3])
ax1.set_ylim([1000, 14000])
# ax1.ylabel("Latency (ms)")
# ax1.xlabel("Input Throughput (transactions/s)")

fig.supxlabel('Zipfian const')
fig.supylabel('Latency (ms)')



plt.tight_layout()
plt.savefig("zipfian.pdf")
plt.show()
