from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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


def GetDistributionData(spark, initial_time, final_time, path_prefix='/user/s1840495/WebInsight/'):
    """ Creates the data that can be used to plot distribution of labels.

    Args:
        spark (SparkContext):
        initial_time (tuple<year:integer, month:integer, day:integer>): First fetch that is used.
        final_time (tuple<year:integer, month:integer, day:integer>): Final fetch that is used as label.
        path_prefix (str, optional): Base path. Defaults to '/user/s1840495/WebInsight/'.
    """
    initial_path = CreatePath(initial_time[0], initial_time[1], initial_time[2], path_prefix)
    final_path = CreatePath(final_time[0], final_time[1], final_time[2], path_prefix)

    raw_data = spark.read.parquet(initial_path)
    final_df_path = spark.read.parquet(final_path) \
        .where(col('url').isNotNull()) \
        .where(col('fetch.contentLength') > 0) \
        .where(col('fetch.fetchDate').isNotNull()) \
        .select(
            'url',
            'fetchMon',
            'fetchDay',
            'history.changeCount',
            'history.fetchCount'
            ) \
        .where(col('fetchMon') == final_time[0]) \
        .where(col('fetchDay') == final_time[1]) \
        .drop('fetchMon') \
        .drop('fetchDay')

    df = raw_data.join(final_df_path, on='url', how='inner')
    return df


if __name__ == "__main__":
    print("Starting distribution preparation.")
    spark = SparkSession.builder.getOrCreate()
    df_labeled = GetDistributionData(spark, (2020, 7, 13), (2020, 9, 14))
    df_labeled.show()
    print("Finished preparation.")
