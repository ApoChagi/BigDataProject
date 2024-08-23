import pyarrow.dataset as ds

# Adjust input and output paths
in_path = "path/to/input/file.csv"
out_path = "outputFileName"

# Load csv into a pandas Dataset
data = ds.dataset(in_path, format='csv')

# Write parquet
ds.write_dataset(data, out_path, format = "parquet",
                 max_rows_per_file = 2300000)
