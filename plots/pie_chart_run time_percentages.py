import matplotlib.pyplot as plt
import numpy as np

from matplotlib import rcParams, rc

rcParams['figure.figsize'] = [7, 6]
plt.rcParams.update({'font.size': 16})

y = [87.13, 4.76, 4.43, 3.67]
mylabels = ["Function Execution", "Sequencing", "Transactional Logic", "Snapshots"]
mycolors = ["#440154", "#31688e", "#35b779", "#fde725"]


pie = plt.pie(y, labels=[f'{perc}%' for perc in y], colors=mycolors)
plt.legend(pie[0], mylabels, bbox_to_anchor=(0.5, 0.1), loc="center",
           bbox_transform=plt.gcf().transFigure, ncol=2)

my_circle = plt.Circle((0, 0), 0.7, color='white')
p = plt.gcf()
p.gca().add_artist(my_circle)


plt.tight_layout()
plt.savefig("runtime_percentages.pdf")
plt.show()
