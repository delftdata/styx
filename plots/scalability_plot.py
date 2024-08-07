from scipy.interpolate import interp1d

import matplotlib.pyplot as plt
from matplotlib import rcParams

rcParams['figure.figsize'] = [12, 5]
plt.rcParams.update({'font.size': 18})
x = [2, 4, 6, 8, 10, 12, 14, 16, 24, 32]

y_styx_mp_prc = {
    #        2        4       6       8      10      12      14      16        24      32
    "0%": [15_517, 30_000, 41_052, 55_862, 68_421, 82_758, 95_103, 108_421, 156_610, 198_493],
    "20%": [13_220, 23_571, 36_428, 48_214, 57_857, 68_421, 79_322, 88_928, 125_172, 161_379],
    "50%": [10_344, 20_357, 31_578, 41_454, 51_272, 59_857, 67_368, 77_894, 111_578, 136_842],
    "100%": [8_275, 16_842, 24_444, 32_727, 42_222, 49_285, 54_642, 61_071, 89_636, 105_762]
}

y_interp_0 = interp1d(x, y_styx_mp_prc["0%"], bounds_error=False, fill_value="extrapolate")
y_interp_20 = interp1d(x, y_styx_mp_prc["20%"], bounds_error=False, fill_value="extrapolate")
y_interp_50 = interp1d(x, y_styx_mp_prc["50%"], bounds_error=False, fill_value="extrapolate")
y_interp_100 = interp1d(x, y_styx_mp_prc["100%"], bounds_error=False, fill_value="extrapolate")

print(y_interp_0(24), y_interp_0(64))
print(y_interp_20(24), y_interp_20(64))
print(y_interp_50(24), y_interp_50(64))
print(y_interp_100(24), y_interp_100(64))


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
plt.yticks([50_000, 100_000, 150_000, 200_000],
           ["50K", "100K", "150K", "200K"])
plt.xticks(x)
# plt.xticks(x)
# # plt.xlim([1, 20])
# plt.ylim([1, 20])
plt.ylabel("Throughput (TPS)")
plt.xlabel("# Workers")
plt.tight_layout()
plt.savefig("scalability.pdf")
plt.show()
