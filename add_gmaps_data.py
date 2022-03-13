import numpy as np
import pandas as pd
import requests
import googlemaps
from os import path
from tqdm import tqdm
import configparser

# Parse config file 
config_parser = configparser.ConfigParser()
config_parser.read("config/gmaps.config")
config = config_parser["Geolocation"]

# Define gmaps client 
gmaps = googlemaps.Client(**config)

# Define file paths 
filepath = "data/grid-metering-point-dump-plus-zipcode.csv"
new_filepath = "data/grid-metering-point-dump-plus-zipcode-GMAPS.csv"

# Read the new file if it exists 
if path.exists(new_filepath):
    df = pd.read_csv(new_filepath)
    
else:
    df = pd.read_csv(filepath)
        
    df["full_address"] = df["street_address"] + ", " + df["name"]
    df["lat"] = np.nan
    df["lng"] = np.nan


# Loop through rows 
for key, row in tqdm(df.iterrows(), total=df.shape[0]):

    if key % 100 == 0:
        print("Current row: " + str(key))

    # If latitude and longitude is not set 
    if np.isnan(df.at[key, "lat"]) and np.isnan(df.at[key,"lng"]):

        # Get geocode from gmaps
        geo = gmaps.geocode(df.at[key, "full_address"])

        # Add the results to the row 
        try: 
            df.at[key, "full_address"] = geo[0]["formatted_address"]
            df.at[key,"lat"] =geo[0]["geometry"]["location"]["lat"]
            df.at[key,"lng"] = geo[0]["geometry"]["location"]["lng"]
        except:
            print("Error in row: " + str(key))
            pass
    else: 
        pass
   

df.to_csv(new_filepath)
