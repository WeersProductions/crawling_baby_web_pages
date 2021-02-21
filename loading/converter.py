from pyspark.sql import SparkSession


# If true, the original data is modified before writing back to disk.
apply_filters = True


def Convert(source, destination, filter):
    # TODO: we should not simply load it.
    # Right now, 'fetch' is a single column as a struct, we might want to convert it to multiple columns for easier parallel analysis.
    df = spark.read.format("json").load(source)
    if apply_filters and filter is not None:
        df = df.filter(filter)
    df.write.parquet(destination)


if __name__ == "__main__":
    print("Converting files.")
    spark = SparkSession.builder.getOrCreate()

    Convert("/data/doina/WebInsight/2020-07-13", "/user/s1840495/WebInsight/2020-07-13.parquet")

    print("Finished converting.")
