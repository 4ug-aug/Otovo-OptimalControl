# Python script to load the dataset

from operator import index
from posixpath import split


def load_dataset(fp):
    """Takes the filepath of our dataset and returns a Dask Dataframe

    Args:
        fp (str): filepath for the dataset

    Returns:
        object: iterable of our dataset
    """

    import dask.dataframe as dd
    
    print(f"load_dataset - Filepath: {fp}")

    dtype={'adjusts_id': 'object',
       'ediel_product_code': 'float64',
       'invoice_item_id': 'object',
       'parent_id': 'object'}

    df = dd.read_csv(fp, sep=";", dtype=dtype)
    print("Loaded dataset to Dask DataFrame")
    return df

def split_dataset(meter_id):
    """ Get a meter_id and create and save a new dataframe based on that

    Args:
        meter_id (str): unique meter_id
    """

    df_meter_id = df[df["meter_id"] == meter_id]
    print(f"Creating new csv for meter: {meter_id}")
    df_meter_id.to_csv(f"/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data/{meter_id}.csv", index= None, header = True,
    single_file=True)

    return None

if __name__ == "__main__":
    dataset_path = "/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data/gridtx-dump.csv"
    df = load_dataset(dataset_path)
    # Create new dataset with only columns
    df = df[['created_at', 'updated_at', 'num_kwh', 'timeslot', 'type',
       'estimation', 'spot_price_no_vat', 'amount_no_vat',
       'vat_percent', 'meter_id', 'kwh_fee_no_vat']]

    import pandas as pd

    print(df.head())
    unique_meter_ids = df["meter_id"].unique().map_partitions(pd.Series.unique).compute()

    print(len(unique_meter_ids))
    
    print(f"Creating datasets for unique meter_ids: \n {unique_meter_ids}")
    for meter in unique_meter_ids:
        split_dataset(meter)
    

