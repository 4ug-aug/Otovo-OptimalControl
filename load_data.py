# Python script to load the dataset

def load_dataset(fp, type=None, sep=";"):
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

def create_datasets_from_meter_id():
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



if __name__ == "__main__":
    dataset_path = "/Users/augusttollerup/Documents/SEM4/Fagprojekt/Project-Work---Bsc.-AIDS/Data/merged_meter_ids.csv"
    df = load_dataset(dataset_path, type="pandas")
    # Create new dataset with only columns
    df = df[['meter_id', 'created_at', 'timeslot']]

    df = df.sort_values("meter_id")
    print(df.head())
    
    

