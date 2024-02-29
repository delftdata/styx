from benchmark.zipfian_generator import ZipfGenerator
import matplotlib.pyplot as plt
import numpy as np

obs = 1_000_000

g_unif = ZipfGenerator(items=1_000_000, zipf_const=0.0)
g_09 = ZipfGenerator(items=1_000_000, zipf_const=0.999)

counts = {}

for _ in range(obs):
    observation = next(g_unif)
    if observation in counts:
        counts[observation] += 1
    else:
        counts[observation] = 1

counts = dict(sorted(counts.items(), key=lambda item: -item[1]))


counts1 = {}

for _ in range(obs):
    observation = next(g_09)
    if observation in counts1:
        counts1[observation] += 1
    else:
        counts1[observation] = 1

counts1 = dict(sorted(counts1.items(), key=lambda item: -item[1]))



print(sum(list(counts1.values())[:100])/obs*100,"%")

# plt.grid(axis="y", linestyle="--")
# plt.plot(np.arange(10_000), counts.values(), "-", color="#31688e", label="Uniform")
# plt.plot(np.arange(10_000), counts1.values(), "-", color="red", label="0.9")
# plt.ylim([0, 1000])
# plt.show()

