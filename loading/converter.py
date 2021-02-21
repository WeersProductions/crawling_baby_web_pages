from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, month, dayofmonth


# If true, the original data is modified before writing back to disk.
apply_filters = True


def Convert(source, destination, filter):
    df = spark.read.format("json").load(source)
    df = df.select(
        df.url,
        df.fetch.contentDigest,
        df.fetch.contentLength,
        df.fetch.textSize,
        df.fetch.textQuality,
        df.fetch.semanticVector,
        df.history.changeCount,
        df.history.fetchCount,
        month(to_timestamp(df.fetch.fetchDate)).alias('fetchMon'),
        dayofmonth(to_timestamp(df.fetch.fetchDate)).alias('fetchDay'),
        df.urlViewInfo.numInLinksInt.alias('internalInLinks'),
        df.urlViewInfo.numInLinksExt.alias('externalInLinks'))
    if apply_filters and filter is not None:
        df = df.filter(filter)
    df.write.parquet(destination)


if __name__ == "__main__":
    print("Converting files.")
    spark = SparkSession.builder.getOrCreate()

    Convert("/data/doina/WebInsight/2020-07-13", "/user/s1840495/WebInsight/2020-07-13.parquet")

    print("Finished converting.")
