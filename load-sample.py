# Python script to load the dataset
from pandas import value_counts


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

    threshold = 100

    value_counts_ = df["meter_id"].value_counts()
    unique_meter_ids_as_frame = value_counts_.to_frame("count").reset_index().rename(columns={"index": 'meter-ids'})

    print(unique_meter_ids_as_frame)

    meter_ids_thresholded = unique_meter_ids_as_frame[unique_meter_ids_as_frame["count"] > threshold]

    
    # print(meter_ids_thresholded[0:3])
    
    # for meter in meter_ids_thresholded:
    #     print(meter)

    # print(meter1_production)
    
