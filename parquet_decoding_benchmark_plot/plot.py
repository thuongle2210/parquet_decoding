import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Read CSV file into DataFrame
df = pd.read_csv('parquet_metadata_benchmark.csv')

# Calculate speedups
df['moonlink'] = 1.0  # baseline always 1
df['parquet56_speedup'] = df['avg_time_thrift_s'] / df['avg_time_parquet56_s']
df['parquet57_speedup'] = df['avg_time_thrift_s'] / df['avg_time_parquet57_s']

# Unique data types and number of plots
data_types = df['data_type'].unique()
num_plots = len(data_types)

fig, axs = plt.subplots(1, num_plots, figsize=(6 * num_plots, 6), sharey=True)

if num_plots == 1:
    axs = [axs]  # Make it iterable even with one subplot

bar_width = 0.25

for ax, dtype in zip(axs, data_types):
    # Filter rows for current data type
    ddf = df[df['data_type'] == dtype]
    index = np.arange(len(ddf))

    # Plot bars for moonlink, parquet56, parquet57 speedups
    ax.bar(index, ddf['moonlink'], bar_width, label="Moonlink's thrift custom decode (baseline)")
    ax.bar(index + bar_width, ddf['parquet56_speedup'], bar_width, label="Parquet decode_metadata v56.0.0")
    ax.bar(index + 2 * bar_width, ddf['parquet57_speedup'], bar_width, label="Parquet decode_metadata v57.0.0")

    ax.set_xlabel('fields_num')
    ax.set_title(f'Speedup Comparison for {dtype}')
    ax.set_xticks(index + bar_width)
    ax.set_xticklabels(ddf['fields_num'])
    ax.legend()
    ax.grid(axis='y')

axs[0].set_ylabel('Speedup (times faster than Moonlink)')

plt.suptitle('Metadata Decoding Time Speedup Comparison')
plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.show()
