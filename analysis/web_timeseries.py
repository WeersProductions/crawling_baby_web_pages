from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_except, size, when, array
from os import path, times

"""
This file is responsible for time serie analysis.
"""

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


def get_timeseries(spark, dates, path_prefix='/user/s1840495/WebInsight/'):
    """
    Gets the initial pages and calculates diffs between all next dates for that page.
    This results in a flow of that page and its behavior.
    """
    print("Time-series using: " + str(dates) + ".")

    diffInternalOutLinkColumns = []
    diffExternalOutLinkColumns = []

    timeseries = None
    for date in dates:
        path = CreatePath(date[0], date[1], date[2], path_prefix)
        date_str = '{0}-{1:02d}-{2:02d}'.format(date[0], date[1], date[2])
        if timeseries is None:
            # This is the first one, let's initialize the start.
            timeseries = spark.read.parquet(path) \
                .where(col('url').isNotNull()) \
                .where(col('fetch_contentLength') > 0) \
                .where(col('fetchMon').isNotNull()) \
                .where(col('fetch_semanticVector').isNotNull()) \
                .select(
                    'url',
                    'fetchMon',
                    'fetchDay',
                    col('internalOutLinks.targetUrl').alias('internalOutLinks'),
                    col('externalOutLinks.targetUrl').alias('externalOutLinks')
                    ) \
                .where(col('fetchMon') == date[1]) \
                .where(col('fetchDay') == date[2]) \
                .drop('fetchMon') \
                .drop('fetchDay')
            timeseries.show()

            continue

        # We have one point in the future, let's create a data point.
        new_df = spark.read.parquet(path) \
            .where(col('fetchMon').isNotNull()) \
            .select(
                'url',
                'fetchMon',
                'fetchDay',
                col('internalOutLinks.targetUrl').alias('newInternalOutLinks'),
                col('externalOutLinks.targetUrl').alias('newExternalOutLinks')
                ) \
            .where(col('fetchMon') == date[1]) \
            .where(col('fetchDay') == date[2]) \
            .drop('fetchMon') \
            .drop('fetchDay')
        print("---: " + date_str + " :----")
        new_df.show()

        print("Starting join!")

        diffInternalOutLinkColumn = date_str + '_diffInternalOutLinks'
        diffInternalOutLinkColumns.append(diffInternalOutLinkColumn)
        diffExternalOutLinkColumn = date_str + '_diffExternalOutLinks'
        diffExternalOutLinkColumns.append(diffExternalOutLinkColumn)

        # Join the two dataframes.
        # If the new data does not contain information about this link, we keep the old internal/external-out link information.
        timeseries = timeseries.join(new_df, on='url', how='inner') \
                        .withColumn(diffExternalOutLinkColumn, size(array_except("newExternalOutLinks", "externalOutLinks"))) \
                        .withColumn(diffInternalOutLinkColumn, size(array_except("newInternalOutLinks", "internalOutLinks"))) \
                        .withColumn('externalOutLinks', col('newExternalOutLinks')) \
                        .drop('newExternalOutLinks') \
                        .withColumn('internalOutLinks', col('newInternalOutLinks')) \
                        .drop('newInternalOutLinks')
                        # .withColumn('externalOutLinks', when(col('newExternalOutLinks').isNull(), col('externalOutLinks')).otherwise(col('newExternalOutLinks'))) \
                        # .withColumn('internalOutLinks', when(col('newInternalOutLinks').isNull(), col('internalOutLinks')).otherwise(col('newInternalOutLinks'))) \
                        # .drop('newExternalOutLinks') \
                        # .drop('newInternalOutLinks')
        print("Timeseries count: " + str(timeseries.count()))
        timeseries.show()

    # Cleanup. Merge columns together to arrays.
    timeseries = timeseries.select('url',
                array(*diffInternalOutLinkColumns).alias("diffInternalOutLinks"),
                array(*diffExternalOutLinkColumns).alias("diffExternalOutLinks"))

    print("Finished creation of timeseries.")
    return timeseries


def check_total_data(spark, dates, path_prefix='/user/s1840495/WebInsight/'):
    # Check how many pages exist in all data sets and how many are left after comining.
    existing_urls = None
    for date in dates:
        path = CreatePath(date[0], date[1], date[2], path_prefix)
        date_str = '{0}-{1:02d}-{2:02d}'.format(date[0], date[1], date[2])

        print("---: " + date_str + " :----")
        df = spark.read.parquet(path) \
                .where(col('url').isNotNull()) \
                .where(col('fetch_contentLength') > 0) \
                .where(col('fetchMon').isNotNull()) \
                .where(col('fetch_semanticVector').isNotNull()) \
                .where(col('fetchMon') == date[1]) \
                .where(col('fetchDay') == date[2]) \
                .select('url')
        print('Count: ' + str(df.count()))
        if existing_urls is None:
            existing_urls = df
            continue

        existing_urls = existing_urls.join(df, on='url', how='inner')
        print('New count: ' + str(existing_urls.count()))
    print("Final count: " + str(existing_urls.count()))


def main(spark, dates):
    timeseries = get_timeseries(spark, dates)
    timeseries.show(truncate=False)
    timeseries.write.mode("overwrite").parquet("WebInsight/analysis/result/web_timeseries.parquet")


if __name__ == "__main__":
    """
    Run using: spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=20 --executor-memory 7g --driver-memory 7g --conf spark.yarn.maxAppAttempts=1 analysis/web_timeseries.py

    Outputs 1 parquet files:
    - WebInsight/analysis/result/web_timeseries.parquet
    """
    spark = SparkSession.builder.getOrCreate()
    dates = [(2020, 7, 13),
        (2020, 7, 20),
        (2020, 7, 28),
        (2020, 8, 6),
        (2020, 8, 11),
        (2020, 8, 17),
        (2020, 8, 27),
        (2020, 9, 1),
        (2020, 9, 7),
        (2020, 9, 14)]

    main(spark, dates)
