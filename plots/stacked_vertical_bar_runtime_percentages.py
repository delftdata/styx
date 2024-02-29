import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib import rcParams

rcParams['figure.figsize'] = [12, 6]
plt.rcParams.update({'font.size': 22})

y = {"Styx": [2.27, 95.56, 2.17],
     "T-Statefun": [2.23, 74.29, 23.48],
     "Boki": [3.34, 28.96, 67.71],
     "Beldi": [0.68, 38.45, 60.87]}

mylabels = ["Function Logic", "Networking", "State Access"]
mycolors = ["#0072B2", "#E69F00", "#009E73"]

fig, ax = plt.subplots(4, 1)

start = 0
x1 = y["Styx"][0]
x2 = y["Styx"][1]
x3 = y["Styx"][2]

ax[0].broken_barh([(start, x1),
                (x1, x1+x2),
                (x1+x2, x1+x2+x3)],
               [12, 12], facecolors=(mycolors[0], mycolors[1], mycolors[2]))
ax[0].set_ylim(5, 15)
ax[0].set_xlim(0, 100)
ax[0].spines['left'].set_visible(False)
ax[0].spines['bottom'].set_visible(False)
ax[0].spines['top'].set_visible(False)
ax[0].spines['right'].set_visible(False)
ax[0].set_yticks([])
ax[0].set_xticks([])
ax[0].title.set_text('Styx')
ax[0].set_axisbelow(True)

ax[0].text(0, 15.2, f"{y["Styx"][0]}%", color=mycolors[0])
ax[0].text(27, 15.2, f"{y["Styx"][1]}%", color=mycolors[1])
ax[0].text(90, 15.2, f"{y["Styx"][2]}%", color=mycolors[2])

###

x1 = y["T-Statefun"][0]
x2 = y["T-Statefun"][1]
x3 = y["T-Statefun"][2]

ax[1].broken_barh([(start, x1),
                (x1, x1+x2),
                (x1+x2, x1+x2+x3)],
               [12, 12], facecolors=(mycolors[0], mycolors[1], mycolors[2]))
ax[1].set_ylim(5, 15)
ax[1].set_xlim(0, 100)
ax[1].spines['left'].set_visible(False)
ax[1].spines['bottom'].set_visible(False)
ax[1].spines['top'].set_visible(False)
ax[1].spines['right'].set_visible(False)
ax[1].set_yticks([])
ax[1].set_xticks([])
ax[1].title.set_text('T-Statefun')
ax[1].set_axisbelow(True)

ax[1].text(0, 15.2, f"{y["T-Statefun"][0]}%", color=mycolors[0])
ax[1].text(27, 15.2, f"{y["T-Statefun"][1]}%", color=mycolors[1])
ax[1].text(87, 15.2, f"{y["T-Statefun"][2]}%", color=mycolors[2])

###

x1 = y["Boki"][0]
x2 = y["Boki"][1]
x3 = y["Boki"][2]

ax[2].broken_barh([(start, x1),
                (x1, x1+x2),
                (x1+x2, x1+x2+x3)],
               [12, 12], facecolors=(mycolors[0], mycolors[1], mycolors[2]))
ax[2].set_ylim(5, 15)
ax[2].set_xlim(0, 100)
ax[2].spines['left'].set_visible(False)
ax[2].spines['bottom'].set_visible(False)
ax[2].spines['top'].set_visible(False)
ax[2].spines['right'].set_visible(False)
ax[2].set_yticks([])
ax[2].set_xticks([])
ax[2].title.set_text('Boki')

ax[2].set_axisbelow(True)

ax[2].text(0, 15.2, f"{y["Boki"][0]}%", color=mycolors[0])
ax[2].text(27, 15.2, f"{y["Boki"][1]}%", color=mycolors[1])
ax[2].text(87, 15.2, f"{y["Boki"][2]}%", color=mycolors[2])

###

x1 = y["Beldi"][0]
x2 = y["Beldi"][1]
x3 = y["Beldi"][2]

ax[3].broken_barh([(start, x1),
                (x1, x1+x2),
                (x1+x2, x1+x2+x3)],
               [12, 12], facecolors=(mycolors[0], mycolors[1], mycolors[2]))
ax[3].set_ylim(5, 15)
ax[3].set_xlim(0, 100)
ax[3].spines['left'].set_visible(False)
ax[3].spines['bottom'].set_visible(False)
ax[3].spines['top'].set_visible(False)
ax[3].spines['right'].set_visible(False)
ax[3].set_yticks([])
ax[3].set_xticks([])
ax[3].title.set_text('Beldi')

ax[3].set_axisbelow(True)

ax[3].text(0, 15.2, f"{y["Beldi"][0]}%", color=mycolors[0])
ax[3].text(27, 15.2, f"{y["Beldi"][1]}%", color=mycolors[1])
ax[3].text(87, 15.2, f"{y["Beldi"][2]}%", color=mycolors[2])

leg1 = mpatches.Patch(color=mycolors[0], label=mylabels[0])
leg2 = mpatches.Patch(color=mycolors[1], label=mylabels[1])
leg3 = mpatches.Patch(color=mycolors[2], label=mylabels[2])

fig.legend(handles=[leg1, leg2, leg3], bbox_to_anchor=(0.5, 0.1), loc="center",  ncol=3)

plt.savefig("runtime_percentages.pdf")
plt.show()
