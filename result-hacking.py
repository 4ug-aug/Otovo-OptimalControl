# Load peak hour price data and meter-id data
import pandas as pd
import numpy as np
import json
from tqdm import tqdm

# Load production and consumption for the cleaned meters 
df_prod = pd.read_csv("data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-PROD.csv")
df_cons = pd.read_csv("data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-CONS.csv")

# Load json file hour_lookup_price.json
with open('data/hour_lookup_price_dict.json') as json_file:
    hour_lookup_price = json.load(json_file)

meter_ids_list = df_prod.meter_id.unique()

# Calculate the summed production and consumption for each meter_id
df = pd.DataFrame(columns=['meter_id', 'consumption', 'production', 'difference', "percentage", "start", "end"])
lowest_difference = 10000000
lowest_difference_meter_id = None
for meter_id in tqdm(meter_ids_list):
    df_prod_meter = df_prod[df_prod.meter_id == meter_id]
    df_cons_meter = df_cons[df_cons.meter_id == meter_id]
    df_prod_meter_sum = df_prod_meter["num_kwh"].sum()
    df_cons_meter_sum = df_cons_meter["num_kwh"].sum()
    difference = df_cons_meter_sum - df_prod_meter_sum
    start = df_cons_meter["timeslot"].iloc[0]
    end = df_cons_meter["timeslot"].iloc[-1]
    # Percentage
    percentage = df_prod_meter_sum / df_cons_meter_sum * 100
    # Add to dataframe
    df = df.append({'meter_id': meter_id, 'consumption': df_cons_meter_sum, 'production': df_prod_meter_sum, 'difference': difference, 'percentage': percentage, 'start': start, "end": end}, ignore_index=True)


# Sort by difference
df = df.sort_values(by=['percentage'], ascending=False)
# Save to csv
df.to_csv("data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-PROD-CONS-DIFFERENCE.csv", index=False)