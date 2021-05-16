from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_except, size
from os import path


def CreatePath(year, month, day, path_prefix, file_format='.parquet'):
    """ Create a path from the day and month components.

    Args:
        day (Integer): Day of the fetch
        month (Integer): Month of the fetch
        path_prefix (String): Base path

    Returns:
        string: absolute path to the file.
    """
    return path.join(path_prefix, '{0}-{1:02d}-{2:02d}{3}'.format(year, month, day, file_format))


def GetLabeledData(spark, initial_time, final_time, path_prefix='/user/s1840495/WebInsight/'):
    """ Uses final_time to create labels for initial_time.

    Args:
        spark (SparkContext):
        initial_time (tuple<year:integer, month:integer, day:integer>): First fetch that is used.
        final_time (tuple<year:integer, month:integer, day:integer>): Final fetch that is used as label.
        path_prefix (str, optional): Base path. Defaults to '/user/s1840495/WebInsight/'.
    """
    initial_path = CreatePath(initial_time[0], initial_time[1], initial_time[2], path_prefix)
    final_path = CreatePath(final_time[0], final_time[1], final_time[2], path_prefix)
    print("Starting GetLabeledData; initial_path:", initial_path, "; final_path: ", final_path)

    # Drop label data.
    initial_data = spark.read.parquet(initial_path) \
        .drop('history_changeCount') \
        .drop('history_fetchCount')

    final_data = spark.read.parquet(final_path) \
        .where(col('url').isNotNull()) \
        .where(col('fetch_contentLength') > 0) \
        .where(col('fetchMon').isNotNull()) \
        .where(col('history_fetchCount') == 10) \
        .select(
            'url',
            'fetchMon',
            'fetchDay',
            'history_changeCount',
            'history_fetchCount',
            col('internalOutLinks.targetUrl').alias('finalInternalOutLinks'),
            col('externalOutLinks.targetUrl').alias('finalExternalOutLinks')
            ) \
        .where(col('fetchMon') == final_time[1]) \
        .where(col('fetchDay') == final_time[2]) \
        .drop('fetchMon') \
        .drop('fetchDay')

    df = initial_data.join(final_data, on='url', how='inner') \
        .withColumn('diffExternalOutLinks', size(array_except("finalExternalOutLinks", "externalOutLinks.targetUrl"))) \
        .withColumn('diffInternalOutLinks', size(array_except("finalInternalOutLinks", "internalOutLinks.targetUrl")))
    print("Finished GetLabeledData")
    return df


def GetLabelDistribution(spark, labeledData):
    labelDistribution = labeledData.groupBy(col('history_changeCount')).count()
    return labelDistribution


def GetFetchDistribution(spark, labeledData):
    fetchDistribution = labeledData.groupBy(col('history_fetchCount')).count()
    return fetchDistribution


if __name__ == "__main__":
    """
    Run using: spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=20 --conf spark.yarn.maxAppAttempts=1 analysis/distribution_plot.py
    """
    print("Starting distribution preparation.")
    spark = SparkSession.builder.getOrCreate()
    df_labeled = GetLabeledData(spark, (2020, 7, 13), (2020, 9, 14))
    df_labeled.write.mode('overwrite').parquet('WebInsight/analysis/result/labeled_07-13_09-14.parquet')
    df_labeled.show()

    labelDistribution = GetLabelDistribution(spark, df_labeled)
    labelDistribution.write.mode('overwrite').parquet('WebInsight/analysis/result/labelDistribution.parquet')

    fetchDistribution = GetFetchDistribution(spark, df_labeled)
    fetchDistribution.write.mode('overwrite').parquet('WebInsight/analysis/result/fetchDistribution.parquet')

    print("Finished preparation.")
