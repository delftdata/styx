from matplotlib import rcParams
import matplotlib.pyplot as plt
import numpy as np

sys_name = "SysX"

rcParams["figure.figsize"] = [12, 5]
# plt.rcParams["figure.autolayout"] = True
plt.rcParams.update({"font.size": 20})

beldi_med_lat = 147.15
boki_med_lat = 32.88
styx_med_lat = 15
tstatefun_med_lat = 124

y = {sys_name: [2.27 / 100 * styx_med_lat,
              95.56 / 100 * styx_med_lat,
              2.17 / 100 * styx_med_lat],
     "Boki": [3.34 / 100 * boki_med_lat,
              48.96 / 100 * boki_med_lat,
              47.71 / 100 * boki_med_lat],
     "T-Statefun": [2.23 / 100 * tstatefun_med_lat,
                    74.29 / 100 * tstatefun_med_lat,
                    23.48 / 100 * tstatefun_med_lat],
     "Beldi": [0.68 / 100 * beldi_med_lat,
               38.45 / 100 * beldi_med_lat,
               60.87 / 100 * beldi_med_lat]}

mylabels = ["Function Execution", "Networking", "State Access"]
mycolors = ["#332288", "#BD7105", "#005F20"]

# create data
x = list(y.keys())
y1 = np.array([y[sys_name][0], y["Boki"][0], y["T-Statefun"][0], y["Beldi"][0]])
y2 = np.array([y[sys_name][1], y["Boki"][1], y["T-Statefun"][1], y["Beldi"][1]])
y3 = np.array([y[sys_name][2], y["Boki"][2], y["T-Statefun"][2], y["Beldi"][2]])

# plot bars in stack manner
bar_width = 0.15
plt.bar(x, y1, color=mycolors[0], width=bar_width, alpha=0.9)
plt.bar(x, y2, bottom=y1, color=mycolors[1], width=bar_width, alpha=0.9)
plt.bar(x, y3, bottom=y1 + y2, color=mycolors[2], width=bar_width, alpha=0.9)

plt.ylabel("Median Latency@100TPS (ms)")
plt.legend(mylabels, loc="upper left", ncol=1, bbox_to_anchor=(0.025, 1.0), frameon=False)
plt.tight_layout()
plt.savefig("runtime_percentages.pdf")
plt.show()
