import pandas as pd
import numpy as np
from ast import literal_eval
import json
import matplotlib.pyplot as plt

meter_data = pd.read_csv("data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-PROD.csv")

timeline = meter_data[meter_data["meter_id"] == "e882f9a7-f1de-4419-9869-7339be303281"]["timeslot"]

# Read data/dp/actions_small_graph_new_meter_id.txt
with open('data/dp/actions_small_graph_new_meter_id.txt', 'r') as f:
    actions = f.read()

# Load data/hour_lookup_price_dict.json
with open('data/nordpool_hour_lookup_price_dict.json', 'r') as f:
    hour_lookup_price_dict = json.load(f)

# Convert string to list of lists
actions = literal_eval(actions)

# loop through actions
cost = 0
cost_list = []
for idx, action in enumerate(actions):
    # Get day and hour of action
    timestamp = pd.to_datetime(timeline.iloc[idx])
    day = str(358 if timestamp.dayofyear > 358 else timestamp.day_of_year)
    hour = str(timestamp.hour)
    # Get price of hour
    price = hour_lookup_price_dict[day][hour]

    # Get cost of action
    buy = action[0]
    sell = action[1]
    cost += buy * price - sell * price*0.1
    cost_list.append(cost)

print(f"Cost: ", cost)