import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

df_meters_id = pd.read_csv("data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-PROD-CONS-DIFFERENCE.csv")
df_meters_prod = pd.read_csv("data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-PROD.csv")
df_meters_cons = pd.read_csv("data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-CONS.csv")

df_meters = pd.DataFrame(columns=["start", "end"])

for i in df_meters_id["meter_id"].unique():
    # Find the minimum start and maximum end of the interval
    start = min(df_meters_prod[df_meters_prod["meter_id"] == i]["timeslot"].min(),df_meters_cons[df_meters_cons["meter_id"] == i]["timeslot"].min())
    end = max(df_meters_prod[df_meters_prod["meter_id"] == i]["timeslot"].max(),df_meters_cons[df_meters_cons["meter_id"] == i]["timeslot"].max())

    # Add the interval to the dataframe
    df_meters = df_meters.append({"start": start, "end": end}, ignore_index=True)

# Convert end and time to year and month
df_meters["start"] = pd.to_datetime(df_meters["start"], format="%Y-%m")
df_meters["end"] = pd.to_datetime(df_meters["end"], format="%Y-%m")

# Create plot with horizontal lines for each meters start and end
plt.figure(figsize=(10,8))
for i in range(len(df_meters)):
    plt.hlines(i, df_meters['start'][i], df_meters['end'][i], alpha=0.6)

# Rotate x
plt.xticks(rotation=90)

# Add labels
plt.xlabel("Time")
plt.ylabel("Meter index value")

# Add title
plt.title("Meters Intervals")

plt.show()
