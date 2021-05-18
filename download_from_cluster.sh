# Used to upload all pyspark files to the cluster to correct folders.
# Note: folders should be created on server side before this script runs.
# Format:
# Root/
#   ^--- analysis/
#   ^--- loading/
#   ^--- util/

USER="s1840495"
TARGET_FOLDER="WebInsight"

mkdir -p ./analysis/local/data/
scp -r ${USER}@ctit013.ewi.utwente.nl:${TARGET_FOLDER}/analysis/result/labeled_07-13_09-14.parquet ./analysis/local/data/labeled_07-13_09-14.parquet
