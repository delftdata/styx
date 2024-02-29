import matplotlib.pyplot as plt
import pandas as pd


input_msgs = pd.read_csv('client_requests.csv')

timestamps = sorted(list(pd.read_csv('client_requests.csv')['timestamp']))

starting_timestamp = timestamps[0]

normalized_timestamps = [timestamp - starting_timestamp for timestamp in timestamps]

bins = range(min(normalized_timestamps), max(normalized_timestamps) + 1000, 1000)

plt.hist(normalized_timestamps, bins=bins)
plt.show()
