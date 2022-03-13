# Python script to load the dataset
def load_dataset(fp):
    """Takes the filepath of our dataset and returns a Dask Dataframe

    Args:
        fp (str): filepath for the dataset

    Returns:
        object: iterable of our dataset
    """

    import pandas as pd

    print(f"load_dataset_with_chunks - Filepath: {fp}")
    df = pd.read_csv(fp, sep = ";", index_col=["timeslot"])
    print("Loaded dataset to Dask DataFrame")
    return df


if __name__ == "__main__":
    import pandas as pd
    dataset_path = "/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data/sample-csv.csv"
    df = load_dataset(dataset_path)
    df = df.sort_values(by=["meter_id"])

    meter1 = df.loc[df["meter_id"] == "007039d0-81e5-4d4f-b300-a27a4f0b6512"]
    meter1_production = meter1.loc[meter1["type"] == "production"]

    
    # print(meter1_production)
    
