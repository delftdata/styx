import matplotlib.pyplot as plt
from matplotlib import rcParams

rcParams['figure.figsize'] = [12, 5]
plt.rcParams.update({'font.size': 18})
x = [2, 4, 6, 8, 10, 12, 14, 16]

y_styx_mp_prc = {
    "0%": [8_852, 17_288, 27_169, 36_226, 44_444, 53_207, 62_222, 68_421],
    "20%": [8_888, 16_071, 24_444, 34_444, 43_333, 52_222, 57_818, 65_357],
    "50%": [8_421, 17_454, 27_169, 35_094, 41_454, 50_000, 57_857, 63_214],
    "100%": [8_421, 17_777, 24_000, 33_818, 41_111, 49_285, 55_789, 62_181]
}

metadata = {
    "0%": {"marker": "-o",
           "color": "#332288"},
    "20%": {"marker": "-*",
            "color": "#882255"},
    "50%": {"marker": "-^",
            "color": "#BD7105"},
    "100%": {"marker": "-X",
             "color": "#005F20"},
}

plt.grid(linestyle="--")
for mp_prc, measurements in y_styx_mp_prc.items():
    plt.plot(x, measurements, metadata[mp_prc]["marker"], color=metadata[mp_prc]["color"], label=mp_prc)

plt.legend(bbox_to_anchor=(0.5, 1.2), loc="center", ncol=5)
plt.yticks([0, 10_000, 30_000, 50_000, 70_000],
           ["0", "10K", "30K", "50K", "70K"])
# plt.xticks(x)
# # plt.xlim([1, 20])
# plt.ylim([1, 20])
plt.ylabel("Throughput (TPS)")  # Max throughput
plt.xlabel("# Workers")
plt.tight_layout()
plt.savefig("scalability.pdf")
plt.show()
