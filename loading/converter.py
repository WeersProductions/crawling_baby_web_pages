from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, month, dayofmonth, size


# If true, the original data is modified before writing back to disk.
apply_filters = True


def Convert(spark, source, destination, filter=None):
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
        df.urlViewInfo.numInLinksInt.alias('n_internalInLinks'),
        df.urlViewInfo.numInLinksExt.alias('n_externalInLinks'),
        size(df.fetch.internalLinks).alias('n_internalOutLinks'),
        size(df.fetch.externalLinks).alias('n_externalOutLinks'),
        df.fetch.internalLinks.alias('internalOutLinks'),
        df.fetch.externalLinks.alias('externalOutLinks'),
        )
    if apply_filters and filter is not None:
        df = df.filter(filter)
    df.write.mode("overwrite").parquet(destination)


if __name__ == "__main__":
    """
    Run using: spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=20 --conf spark.yarn.maxAppAttempts=1 converter.py
    Converts to:
        root
        |-- url: string (nullable = true)
        |-- fetch.contentDigest: string (nullable = true)
        |-- fetch.contentLength: long (nullable = true)
        |-- fetch.textSize: long (nullable = true)
        |-- fetch.textQuality: double (nullable = true)
        |-- fetch.semanticVector: string (nullable = true)
        |-- history.changeCount: long (nullable = true)
        |-- history.fetchCount: long (nullable = true)
        |-- fetchMon: integer (nullable = true)
        |-- fetchDay: integer (nullable = true)
        |-- internalInLinks: string (nullable = true)
        |-- externalInLinks: string (nullable = true)
    """
    print("Converting files.")
    spark = SparkSession.builder.getOrCreate()

    Convert(spark, "/data/doina/WebInsight/2020-07-13", "/user/s1840495/WebInsight/2020-07-13.parquet")

    print("Finished converting.")
