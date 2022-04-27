from load_data import load_dataset, split_dataset, merge_data_sets
import random
from tqdm import tqdm
import glob
import pandas as pd

random.seed(666)

dataset_path = "/Users/augusttollerup/Documents/SEM4/Fagprojekt/Project-Work---Bsc.-AIDS/Data/gridtx-dump.csv"

# data = load_dataset(dataset_path, "dask")

# meter_ids = list(data["meter_id"].unique().compute())

# random_meter_ids = random.sample(meter_ids, 30)
# print(random_meter_ids)

# print(f"Creating datasets for {len(random_meter_ids)} unique meter ids")
# for meter_id in tqdm(random_meter_ids):
#     split_dataset(data, meter_id)

li = []
fp = "/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data"
all_files = glob.glob(fp + "/*.csv")
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)

frame = pd.concat(li, axis=0, ignore_index=True)
frame.to_csv(f"/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data/random-meter-ids.csv", sep=";")
