from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_replace, udf #, to_timestamp, lag, unix_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, FloatType, ArrayType

from re import sub
from urlparse import urlparse
from time import strptime

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

crawl_initial = [7, 13, "07-13"] # integer tuples (month, day, pretty_string)
crawl_final = [9, 14, "09-14"]

def getPathDepth(url):
	# /en/news/article123456/ has pathDepth 3
	path = urlparse(url).path
	# NB: urlparse("http://www.drive.google.com/en/news/article123456/")
	#     ParseResult(scheme='http', netloc='www.drive.google.com', path='/en/news/article123456/', params='', query='', fragment='')
	path = sub("/$", "", path) # $ matches the end of line
	return path.count("/")

udf_getPathDepth = udf(getPathDepth, IntegerType())

def getDomainDepth(url):
	# drive.google.com has domainDepth 3
	netloc = sub(r"^www.", "", urlparse(url).netloc)
	return len(netloc.split("."))

udf_getDomainDepth = udf(getDomainDepth, IntegerType())

def getTrustRank(struct):
	if not struct: 
		return None
	for item in struct:
		if item['id'] == 'TrustValue':
			return item['value']
	return None

udf_getTrustRank = udf(getTrustRank, StringType())

def numExternalOutLinks(this_url, out_url_array_external):
	if not out_url_array_external: 
		return 0
	#
	# This corrects any misclassified instances; it isn't crucial to do though
	# corrected_out_url_array = []
	# this_netloc = sub(r"^www.", "", urlparse(this_url).netloc)
	# for u in out_url_array_external:
	# 	out_netloc = sub(r"^www.", "", urlparse(u['targetUrl']).netloc)
	# 	if out_netloc != this_netloc:
	# 		corrected_out_url_array.append(u['targetUrl'])
	# return len(corrected_out_url_array)
	#
	return len(out_url_array_external)

udf_numExternalOutLinks = udf(numExternalOutLinks, IntegerType())

def numInternalOutLinks(this_url, out_url_array_internal, out_url_array_external):
	if not out_url_array_internal: 
		num_internal = 0
	else:
		num_internal = len(out_url_array_internal)
	#
	# This corrects any misclassified instances; it isn't crucial to do though
	# if not out_url_array_external:
	# 	return num_internal
	# extra_out_url_array = []
	# this_netloc = sub(r"^www.", "", urlparse(this_url).netloc)
	# for u in out_url_array_external:
	# 	out_netloc = sub(r"^www.", "", urlparse(u['targetUrl']).netloc)
	# 	if out_netloc == this_netloc:
	# 		extra_out_url_array.append(u['targetUrl'])
	# return len(extra_out_url_array) + num_internal
	#
	return num_internal

udf_numInternalOutLinks = udf(numInternalOutLinks, IntegerType())

def reduce_to_mon(string_date):
	ts = strptime(string_date, "%Y-%m-%d %H:%M")
	return ts.tm_mon

def reduce_to_day(string_date):
	ts = strptime(string_date, "%Y-%m-%d %H:%M")
	return ts.tm_mday

udf_mon = udf(reduce_to_mon, IntegerType())
udf_day = udf(reduce_to_day, IntegerType())

# From the very first crawl, simply grab the static page and static network features.
# The .filter() by fetchDate shouldn't be necessary (I haven't checked; it is safe in any case).

features = spark.read.json("/data/doina/WebInsight/2020-" + crawl_initial[2] + "/*.gz") \
	.where(
		(col('url').isNotNull()) & 
		(col('fetch.contentLength') > 0) & 
		(col('fetch.fetchDate').isNotNull())) \
	.select(
		'url', 
		udf_getPathDepth(col('url')).alias('pathDepth'), 
		udf_getDomainDepth(col('url')).alias('domainDepth'), 
		'fetch.language', 
		'fetch.contentLength', 
		'fetch.textSize', 
		'fetch.textQuality', 
		'fetch.semanticVector', 
		udf_mon('fetch.fetchDate').alias('fetchMon'), 
		udf_day('fetch.fetchDate').alias('fetchDay'), 
		udf_numInternalOutLinks(col('url'), col('fetch.internalLinks'), col('fetch.externalLinks')).alias('internalOutLinks'), 
		udf_numExternalOutLinks(col('url'), col('fetch.externalLinks')).alias('externalOutLinks'), 
		col('urlViewInfo.numInLinksInt').alias('internalInLinks'), 
		col('urlViewInfo.numInLinksExt').alias('externalInLinks'), 
		udf_getTrustRank(col('urlViewInfo.metrics.entries')[0]['metrics']['entries']).alias('trustRank')) \
	.filter(
		(col('fetchMon') == crawl_initial[0]) & 
		(col('fetchDay') == crawl_initial[1])) \
	.drop('fetchMon') \
	.drop('fetchDay') \
	.cache()

# From the final crawl, grab only the changeCount and fetchCount.
# The filter by fetchDate is necessary: some ~5% of the pages have a fetchDate that's far off the expected, 
# and probably belong to other crawl files; removing them is safest for now.
# The filter on fetchCount only ensures that the pages have been crawled frequently enough by now, so the
# changeRate can be estimated as well as possible given this data.

targets = spark.read.json("/data/doina/WebInsight/2020-" + crawl_final[2] + "/*.gz") \
	.where(
		(col('url').isNotNull()) & 
		(col('fetch.contentLength') > 0) & 
		(col('fetch.fetchDate').isNotNull())) \
	.select(
		'url', 
		udf_mon('fetch.fetchDate').alias('fetchMon'), 
		udf_day('fetch.fetchDate').alias('fetchDay'), 
		'history.changeCount', 
		'history.fetchCount') \
	.filter(
		(col('fetchMon') == crawl_final[0]) & 
		(col('fetchDay') == crawl_final[1])) \
	.drop('fetchMon') \
	.drop('fetchDay') \
	.filter(
		col('fetchCount') >= 9) \
	.cache()

# This join should be cheap in memory: one assumes there's only one instance of each URL in each crawl.
# Runtime: 5 mins without the coalesce(). The resulting file is 1.2 GB uncompressed, with just under 700k pages.

dataset = features \
	.join(targets, on='url', how='inner') \
	.coalesce(1) \
	.write.json("WebInsight/changeRate_dataset", mode="overwrite")
