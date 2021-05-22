from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, month, dayofmonth, size
from pyspark.sql.types import IntegerType


# If true, the original data is modified before writing back to disk.
apply_filters = True


def Convert(spark, source, destination, filter=None):
    df = spark.read.format("json").load(source)
    df = df.select(
        df.url,
        df.fetch.contentDigest.alias("fetch_contentDigest"),
        df.fetch.contentLength.alias("fetch_contentLength"),
        df.fetch.textSize.alias("fetch_textSize"),
        df.fetch.textQuality.alias("fetch_textQuality"),
        df.fetch.semanticVector.alias("fetch_semanticVector"),
        df.history.changeCount.alias("history_changeCount"),
        df.history.fetchCount.alias("history_fetchCount"),
        month(to_timestamp(df.fetch.fetchDate)).alias('fetchMon'),
        dayofmonth(to_timestamp(df.fetch.fetchDate)).alias('fetchDay'),
        df.urlViewInfo.numInLinksInt.cast(IntegerType()).alias('n_internalInLinks'),
        df.urlViewInfo.numInLinksExt.cast(IntegerType()).alias('n_externalInLinks'),
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
    Run using: spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=20 --conf spark.yarn.maxAppAttempts=1 loading/converter.py
    If you're running a lot of files, memory limit might be a problem. Run using: spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=20 --conf spark.yarn.maxAppAttempts=1 --driver-memory 4G --executor-memory 4G loading/converter.py
    Converts to:
        root
            |-- url: string (nullable = true)
            |-- fetch_contentDigest: string (nullable = true)
            |-- fetch_contentLength: long (nullable = true)
            |-- fetch_textSize: long (nullable = true)
            |-- fetch_textQuality: double (nullable = true)
            |-- fetch_semanticVector: string (nullable = true)
            |-- history_changeCount: long (nullable = true)
            |-- history_fetchCount: long (nullable = true)
            |-- fetchMon: integer (nullable = true)
            |-- fetchDay: integer (nullable = true)
            |-- n_internalInLinks: integer (nullable = true)
            |-- n_externalInLinks: integer (nullable = true)
            |-- n_internalOutLinks: integer (nullable = true)
            |-- n_externalOutLinks: integer (nullable = true)
            |-- internalOutLinks: array (nullable = true)
            |    |-- element: struct (containsNull = true)
            |    |    |-- linkInfo: struct (nullable = true)
            |    |    |    |-- linkQuality: double (nullable = true)
            |    |    |    |-- linkRels: array (nullable = true)
            |    |    |    |    |-- element: string (containsNull = true)
            |    |    |    |-- linkType: string (nullable = true)
            |    |    |    |-- text: string (nullable = true)
            |    |    |-- targetUrl: string (nullable = true)
            |-- externalOutLinks: array (nullable = true)
            |    |-- element: struct (containsNull = true)
            |    |    |-- linkInfo: struct (nullable = true)
            |    |    |    |-- linkQuality: double (nullable = true)
            |    |    |    |-- linkRels: array (nullable = true)
            |    |    |    |    |-- element: string (containsNull = true)
            |    |    |    |-- linkType: string (nullable = true)
            |    |    |    |-- text: string (nullable = true)
            |    |    |-- targetUrl: string (nullable = true)
    """
    print("Converting files.")
    spark = SparkSession.builder.getOrCreate()
    to_be_converted = [
        (2020, 7, 13),
        (2020, 7, 21),
        (2020, 7, 28),
        (2020, 8, 7),
        (2020, 8, 11),
        (2020, 8, 18),
        (2020, 8, 27),
        (2020, 9, 1),
        (2020, 9, 7),
        (2020, 9, 14)
    ]

    # NOTE: the following was done manually:
    """
        hdfs dfs -mv WebInsight/2020-07-21.parquet WebInsight/2020-07-20.parquet
        hdfs dfs -mv WebInsight/2020-08-07.parquet WebInsight/2020-08-06.parquet
        hdfs dfs -mv WebInsight/2020-08-18.parquet WebInsight/2020-08-17.parquet
    """

    for fetch_time in to_be_converted:
        path = "/WebInsight/{0}-{1:02d}-{2:02d}".format(fetch_time[0], fetch_time[1], fetch_time[2])
        source_path = "/data/doina{0}".format(path)
        destination_path = "/user/s1840495{0}{1}".format(path, '.parquet')
        Convert(spark, source_path, destination_path)

    print("Finished converting.")
