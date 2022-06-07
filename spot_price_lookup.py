import pandas as pd
import numpy as np
import os
import datetime
from matplotlib import pyplot as plt
import seaborn as sns
import json

# load dataset
fp = "data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED.csv"
data = pd.read_csv(fp, sep=",")

# Meter with most observations
mm_id = "28ba7f57-6e83-4341-8078-232c1639e4e3"

# df = data[data["meter_id"] == mm_id]
df = data

df_cons = df[df["type"] == "consumption"]

# Define peak hours 8-11 and 16-19
peak_hours = [8, 9, 10, 11, 16, 17, 18, 19]
# All hours
all_hours = list(range(24))
# difference between peak hours and all hours (off_peak_hours)
off_peak_hours = list(set(all_hours) - set(peak_hours))

# Convert timeslot to datetime and set as index
df_cons.index = pd.to_datetime(df_cons['timeslot'], utc=True)

# Sort index 
df_cons = df_cons.sort_index()
df_cons["hour"] = df_cons.index.hour
# Add column to df_cons_spot_price
df_cons["peak_hour"] = np.where(df_cons["hour"].isin(peak_hours), 1, 0)

#Average over peak_hour
df_cons_avg = df_cons.groupby(["peak_hour"]).mean()["spot_price_no_vat"]

# df_cons_avg to dict
avg_dict = df_cons_avg.to_dict()

hour_lookup_price_dict = {}
for hour in all_hours:

    if hour in peak_hours:
        hour_lookup_price_dict[hour] = avg_dict[1]
    elif hour in off_peak_hours:
        hour_lookup_price_dict[hour] = avg_dict[0]


print(hour_lookup_price_dict)
# Save hour_lookup_price_dict to json
with open('data/hour_lookup_price_dict.json', 'w') as fp:
    json.dump(hour_lookup_price_dict, fp)