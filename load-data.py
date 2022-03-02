# Python script to load the dataset

def load_dataset(fp):
    """Takes the filepath of our dataset and returns a Dask Dataframe

    Args:
        fp (str): filepath for the dataset

    Returns:
        object: iterable of our dataset
    """

    import dask.dataframe as dd
    print(f"load_dataset_with_chunks - Filepath: {fp}")
    df = dd.read_csv(fp)
    print("Loaded dataset to Dask DataFrame")
    return df


if __name__ == "__main__":
    dataset_path = "/Users/augusttollerup/Documents/SEM4/Fagprojekt/Data/gridtx-dump.csv"
    df = load_dataset(dataset_path)
    print(df)
