"""
Collection of all auxiliary functions
"""

def add_gmaps_data(fp = "data/grid-metering-point-dump-plus-zipcode.csv", new_fp  = "data/grid-metering-point-dump-plus-zipcode-GMAPS.csv"):
    from asyncio.windows_events import ERROR_CONNECTION_REFUSED
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
    filepath = fp
    new_filepath = new_fp

    # Read the new file if it exists 
    if path.exists(new_filepath):
        df = pd.read_csv(new_filepath)
        
    else:
        df = pd.read_csv(filepath)
            
        df["full_address"] = df["name"] + ", Norway" if type(df["street_address"]) != "str" else df["street_address"] + ", " + df["name"] + ", Norway"

        # df["full_address"] = df["street_address"] + ", " + df["name"] + ", Norway" 
        df["lat"] = np.nan
        df["lng"] = np.nan


    errors = []

    # Loop through rows 
    for key, row in tqdm(df.iterrows(), total=df.shape[0]):

        # if key % 100 == 0:
        #     print("Current row: " + str(key))

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
                errors.append(key)
                pass
        else: 
            pass

    if errors:
        print("There were errors in the following rows: ")
        print(errors)

    df.to_csv(new_filepath)

def load_dataset(fp, type=None, sep=","):
    """Takes the filepath of our dataset and returns a either a Dask or Pandas Dataframe

    Args:
        fp (str):   filepath for the dataset
        type (str): 'pandas' or 'dask'

    Returns:
        object: iterable of our dataset
    """

    import dask.dataframe as dd
    import pandas as pd
    
    print(f"load_dataset - Filepath: {fp}")

    dtype={'adjusts_id': 'object',
       'ediel_product_code': 'float64',
       'invoice_item_id': 'object',
       'parent_id': 'object'}

    if type == "dask":

        df = dd.read_csv(fp, sep=sep, dtype=dtype)
        print("Loaded dataset to Dask DataFrame")
        return df
    elif type == "pandas":
        df = pd.read_csv(fp, sep=sep, dtype=dtype)
        print("Loaded dataset to Pandas DataFrame")
        return df
    else:
        print(f"Invalid choice: {type}, choose between pandas or dask")

def split_dataset(df, meter_id):
    """ Get a meter_id and create and save a new dataframe based on that

    Args:
        meter_id (str): unique meter_id
    """

    df_meter_id = df[df["meter_id"] == meter_id]
    print(f"Creating new csv for meter: {meter_id}")
    df_meter_id.to_csv(f"{meter_id}.csv", index= None, header = True,
    single_file=True)

    return None

def create_datasets_from_meter_id(df):
    import pandas as pd
    from tqdm import tqdm

    threshold = 1e5

    print(df.head())
    # unique_meter_ids = df["meter_id"].unique().map_partitions(pd.Series.unique)
    value_counts_ = df["meter_id"].value_counts()
    unique_meter_ids_as_frame = value_counts_.to_frame("count").reset_index().rename(columns={"index": 'meter-ids'})
    meter_ids_thresholded = unique_meter_ids_as_frame[unique_meter_ids_as_frame["count"] > threshold]["meter-ids"].compute()
    
    print(f"Creating datasets for {len(meter_ids_thresholded)} unique meter ids")
    for meter in tqdm(meter_ids_thresholded):
        split_dataset(meter)

def merge_data_sets(fp, name):
    """Temporary / Minor function to merge the multiple csv files into a single csv
    """
    import glob
    import pandas as pd
    from tqdm import tqdm

    path = fp
    all_files = glob.glob(path + "/*.csv")

    li = []

    for filename in tqdm(all_files):
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)

    frame = pd.concat(li, axis=0, ignore_index=True)
    frame.to_csv(f"/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data/{name}.csv", sep=";")

def get_randommeter_ids(dataset_path = "/Users/augusttollerup/Documents/SEM4/Fagprojekt/Project-Work---Bsc.-AIDS/Data/gridtx-dump.csv"):
    import random
    from tqdm import tqdm
    import glob
    import pandas as pd

    random.seed(666)

    li = []
    fp = "/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data"
    all_files = glob.glob(fp + "/*.csv")
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)

    frame = pd.concat(li, axis=0, ignore_index=True)
    frame.to_csv(f"/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data/random-meter-ids.csv", sep=";")

def meters_2_year_coverage(prod_pf = 'year_coverage_consumption_100.csv', cons_fp = 'year_coverage_production_100.csv'):
    from turtle import pd
    import pandas as pd
    from pprint import pprint

    data = pd.read_csv(prod_pf)
    data2 = pd.read_csv(cons_fp)

    year_coverage_2_cons = data[data['time_between_first_last'] >= 2]['meter_id']
    year_coverage_2_prod = data2[data2['time_between_first_last'] >= 2]['meter_id']

    # Print all meter-ids with time_between_first_last >= 2
    print(len(year_coverage_2_cons))
    print(len(year_coverage_2_prod))

    # print which meters are in consumption but not in production
    difference_cons = set(year_coverage_2_cons).difference(set(year_coverage_2_prod))
    difference_prod = set(year_coverage_2_prod).difference(set(year_coverage_2_cons))

    print("Meter-ids with 2 year coverage in consumption but not in production:")
    pprint(difference_cons)
    print()
    print("Meter-ids with 2 year coverage in production but not in consumption:")
    pprint(difference_prod)

def exploring_dataset(df_fp, fp_df_whole):
    import pandas as pd
    import matplotlib.pyplot as plt
    import plotly.express as px
    import dask.dataframe as dd
    from load_data import load_dataset

    dtype={'adjusts_id': 'object',
        'ediel_product_code': 'float64',
        'invoice_item_id': 'object',
        'parent_id': 'object'}

    df_main = pd.read_csv(df_fp, 
                            delimiter=',', header=0)

    df_whole = load_dataset(fp_df_whole)

    def plot_timeslot():
        """Plot Hourly, Weekly, Monthly and Weekday production, consumption and elcert
        """
        df_copy = df_main
        print(df_copy.columns)

        def dtextract(x):
            df_copy[x] = pd.to_datetime(df_copy[x], utc=True)
            df_copy["year"] = df_copy[x].dt.year
            df_copy["weekly"] = df_copy[x].dt.isocalendar().week
            df_copy['month'] = df_copy[x].dt.month
            df_copy['hour'] = df_copy[x].dt.hour
            df_copy['weekday'] = df_copy[x].dt.weekday

        dtextract('created_at')

        fig, axs = plt.subplots(nrows = 3, ncols = 2)
        df2 = df_copy[df_copy['type']=='production']
        df3 = df_copy[df_copy['type']=='consumption']
        df4 = df_copy[df_copy['type']=='elcert']

        axs[0,0].plot(df2.groupby(df2['month'])["num_kwh"].mean(), color = 'green')
        axs[0,0].plot(df3.groupby(df3['month'])["num_kwh"].mean(), color = 'red')
        axs[0,0].plot(df4.groupby(df4['month'])["num_kwh"].mean(), color = 'blue')
        axs[0,0].set_title("Monthly", fontsize=10)
        axs[0,0].legend(["production","consumption","elcert"])

        axs[0,1].plot(df2.groupby(df2['weekday'])["num_kwh"].mean(), color = 'green')
        axs[0,1].plot(df3.groupby(df3['weekday'])["num_kwh"].mean(), color = 'red')
        axs[0,1].plot(df4.groupby(df4['weekday'])["num_kwh"].mean(), color = 'blue')
        axs[0,1].set_title("Weekday", fontsize=10)
        axs[0,1].legend(["production","consumption","elcert"])

        axs[1,0].plot(df2.groupby(df2['hour'])["num_kwh"].mean(), color = 'green')
        axs[1,0].plot(df3.groupby(df3['hour'])["num_kwh"].mean(), color = 'red')
        axs[1,0].plot(df4.groupby(df4['hour'])["num_kwh"].mean(), color = 'blue')
        axs[1,0].set_title("Hourly", fontsize=10)
        axs[1,0].legend(["production","consumption","elcert"])

        axs[1,1].plot(df2.groupby(df2['weekly'])["num_kwh"].mean(), color = 'green')
        axs[1,1].plot(df3.groupby(df3['weekly'])["num_kwh"].mean(), color = 'red')
        axs[1,1].plot(df4.groupby(df4['weekly'])["num_kwh"].mean(), color = 'blue')
        axs[1,1].set_title("Weekly", fontsize=10)
        axs[1,1].legend(["production","consumption","elcert"])
        
        axs[2,0].plot(df3.groupby(df3['weekly'])["num_kwh"].mean(), color = 'red')
        axs[2,0].plot(df3.groupby(df3['weekly'])["amount_no_vat"].mean(), color = 'green')
        axs[2,0].set_title("Amount no VAT vs Consumption", fontsize=10)
        axs[2,0].legend(["Consumption","Spot Price"])

        axs[2,1].remove()

        plt.show();

    def describe_whole_dataset():
        """Print missing values and Meter ID Value count
        """
        df_copy = df_whole
        print("Missing values in the dataset based on columns:")
        nans = df_copy.isnull().sum().compute()
        print(nans)
        print(f"Total nans: {nans.sum()}")

        print("Different sized meter_id occurences")
        value_counts_ = df_copy["meter_id"].value_counts().compute()
        print(value_counts_)

    def distribution_of_measurements():
        """Plot Distribution of Meter ID Observations
        """
        df_copy = df_whole
        value_counts_ = df_copy["meter_id"].value_counts().compute()
        plot = px.histogram(value_counts_, x="meter_id")
        plot2 = px.box(value_counts_, y="meter_id")
        
        plot.show();
        plot2.show();

def get_series(meter_id, type="prod", start=None, end=None, agg=None):
    """Create Series from meter_id and type of data

    Args:
        meter_id (str): meter-id
        type (str, optional): production or consumption of kwh. Defaults to "prod".
        start (str, optional): timeslot to start series. Defaults to None.
        end (str, optional): timeslot to end series. Defaults to None.
        agg (str, optional): aggregation of data. One of day, week or month. Defaults to None.

    Returns:
        pd.series: series of filtered data
    """
    import pandas as pd
    import numpy as np

    print("Getting series for meter_id: {}".format(meter_id))

    # if start not none
    if start is not None:
        # Convert to datetime
        start = pd.to_datetime(start)
    
    # if end not none
    if end is not None:
        # Convert to datetime
        end = pd.to_datetime(end)

    if type == "prod":
        df_return = df_prod[df_prod["meter_id"] == meter_id]
        # Drop all columns but timeslot and num_kwh_normalized
        df_return = df_return[['timeslot', 'num_kwh']]
        # Set index to timeslot
        # Filter on start and end
        # Convert timeslot to datetime
        df_return["timeslot"] = pd.to_datetime(df_return["timeslot"], utc=True)
        if start is not None and end is not None:
            print("Filtering on start and end: ", start, end)
            try:
                df_return = df_return[(df_return['timeslot'] >= start) & (df_return['timeslot'] <= end)]
            except Exception as e:
                print(e)
                print("No data for this timeslot, timeslot might be incorrect format or out of range:")
                print("Format and range for timeslot: ", df_return.index[0], " ", df_return.index[-1])
                print("Format for input start: ", start)
                print("Format for input end: ", end)
                pass
        elif start is not None:
            print("Filtering on start: ", start)
            try:
                df_return = df_return[(df_return['timeslot'] >= start)]
            except:
                print("No data for this timeslot, timeslot might be incorrect format or out of range:")
                print("Format and range for timeslot: ", df_return.index[0], " ", df_return.index[-1])
                print("Format for input start: ", start)
                pass
        elif end is not None:
            print("Filtering on end: ", end)
            try:
                df_return = df_return[(df_return['timeslot'] <= end)]
            except:
                print("No data for this timeslot, timeslot might be incorrect format or out of range:")
                print("Format and range for timeslot: ", df_return.index[0], " ", df_return.index[-1])
                print("Format for input end: ", end)
                pass

    elif type == "cons":
        df_return = df_cons[df_cons["meter_id"] == meter_id]
        # Drop all columns but timeslot and num_kwh_normalized
        df_return = df_return[['timeslot', 'num_kwh']]
        # Set index to timeslot
        # Filter on start and end
        # Convert timeslot to datetime
        df_return["timeslot"] = pd.to_datetime(df_return["timeslot"], utc=True)
        if start is not None and end is not None:
            print("Filtering on start and end: ", start, end)
            try:
                df_return = df_return[(df_return['timeslot'] >= start) & (df_return['timeslot'] <= end)]
            except Exception as e:
                print(e)
                print("No data for this timeslot, timeslot might be incorrect format or out of range:")
                print("Format and range for timeslot: ", df_return.index[0], " ", df_return.index[-1])
                print("Format for input start: ", start)
                print("Format for input end: ", end)
                pass
        elif start is not None:
            print("Filtering on start: ", start)
            try:
                df_return = df_return[(df_return['timeslot'] >= start)]
            except:
                print("No data for this timeslot, timeslot might be incorrect format or out of range:")
                print("Format and range for timeslot: ", df_return.index[0], " ", df_return.index[-1])
                print("Format for input start: ", start)
                pass
        elif end is not None:
            print("Filtering on end: ", end)
            try:
                df_return = df_return[(df_return['timeslot'] <= end)]
            except:
                print("No data for this timeslot, timeslot might be incorrect format or out of range:")
                print("Format and range for timeslot: ", df_return.index[0], " ", df_return.index[-1])
                print("Format for input end: ", end)
                pass

    df_return = df_return.set_index("timeslot").sort_index()

    # If agg is not none
    if agg is not None:
        if agg == "day":
            df_return = df_return.resample("D").sum()
        elif agg == "week":
            df_return = df_return.resample("W").sum()
        elif agg == "month":
            df_return = df_return.resample("M").sum()
        else:
            print("Aggregation not supported")
            return None

    # Return series
    return df_return