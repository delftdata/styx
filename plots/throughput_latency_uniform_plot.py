import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, rc

rcParams['figure.figsize'] = [9, 8]
plt.rcParams.update({'font.size': 16})
x = [100, 300, 500, 700, 1000, 1200, 1500, 2000, 3000, 6000, 8000, 10000, 12000]
y_styx_50 = [21, 28, 25, 26, 26, None, 24, 23, 22, 33, 50, 127, 639]
y_beldi_50 = [269.90, 267.25, 220.37, 69.87, 78.17, 71.78, 256.39, None, None, None, None, None, None]
y_statefun_50 = [165, 169, 184, 164, 179, 191, 518, 733, 14199, None, None, None, None]

y_styx_99 = [40, 48, 44, 50, 50, None, 44, 41, 41, 67, 281, 695, 1760]
y_beldi_99 = [400.77, 431.11, 405.97, 21276.83, 46758.28, 53652.28, 57768.81, None, None, None, None, None, None]
y_statefun_99 = [327.01, 509, 608.02, 609, 742, 1002, 1346, 4316, 26266.01, None, None, None, None]

fig, (ax1, ax0) = plt.subplots(2, 1)
ax0.grid(axis="y", linestyle="--")
ax0.plot(x, y_styx_50, "-o", color="#31688e", label="Styx 50p")
ax0.plot(x, y_styx_99, "--", marker="o", color="#31688e", label="Styx 99p")
ax0.plot(x, y_beldi_50, "-^", color="#35b779", label="Beldi 50p")
ax0.plot(x, y_beldi_99, "--", marker="^", color="#35b779", label="Beldi 99p")
ax0.plot(x, y_statefun_50, "-x", color="#fde725", label="T-Statefun 50p")
ax0.plot(x, y_statefun_99, "--", marker="x", color="#fde725", label="T-Statefun 99p")
# ax0.legend()
# ax0.set_xlim([0, 3])
ax0.set_ylim([0, 1000])             
# ax0.ylabel("Latency (ms)")
# ax0.xlabel("Input Throughput (transactions/s)")

ax1.grid(axis="y", linestyle="--")
ax1.plot(x, y_styx_50, "-o", color="#31688e", label="Styx 50p")
ax1.plot(x, y_styx_99, "--", marker="o", color="#31688e", label="Styx 99p")
ax1.plot(x, y_beldi_50, "-^", color="#35b779", label="Beldi 50p")
ax1.plot(x, y_beldi_99, "--", marker="^", color="#35b779", label="Beldi 99p")
ax1.plot(x, y_statefun_50, "-x", color="#fde725", label="T-Statefun 50p")
ax1.plot(x, y_statefun_99, "--", marker="x", color="#fde725", label="T-Statefun 99p")
ax1.legend(bbox_to_anchor=(0.5, 1.2), loc="center", ncol=3)
# ax0.set_xlim([0, 3])
ax1.set_ylim([1000, 70000])
# ax1.ylabel("Latency (ms)")
# ax1.xlabel("Input Throughput (transactions/s)")

fig.supxlabel('Input Throughput (transactions/s)')
fig.supylabel('Latency (ms)')



plt.tight_layout()
plt.savefig("throughput_latency_uniform.pdf")
plt.show()
