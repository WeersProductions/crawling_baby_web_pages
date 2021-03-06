import pickle
from sys import argv
from pyspark.sql import SparkSession
import numpy as np

if __name__ == "__main__":
    """
    Run using: spark-submit <PROJECT_NAME>/util/parquet_file_to_pickle.py "A FILE PATH"
    Note: Folders of output file should be created beforehand. Run from root to get consistent results (so do spark-submit from root).
    <PROJECT_NAME> = WebInsight in the current configuration.
    """
    print("Converting parquet file to pickle.")
    spark = SparkSession.builder.getOrCreate()

    if len(argv) >= 2:
        print("Converting %s" % argv[1])
        df = spark.read.parquet(argv[1])

        if len(argv) > 2:
            target = argv[2]
        else:
            target = argv[1][:-8] + ".pickle"  # Remove .parquet add .pickle

        # Convert to numpy to reduce memory usage.
        data_points = np.array(df.collect())

        pickle_data = {
            "column_names": df.columns,
            "data_points": data_points
        }

        pickle.dump(pickle_data, open(target, "wb"))
        print("Saved at %s!" % target)
    else:
        print("Please provide an input .parquet file.")
