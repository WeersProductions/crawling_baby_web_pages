# Used to upload all pyspark files to the cluster to correct folders.
# Note: folders should be created on server side before this script runs.
# Format:
# Root/
#   ^--- analysis/
#   ^--- loading/
#   ^--- util/

USER="s1840495"
TARGET_FOLDER="WebInsight"

# scp ./analysis/regression.py ${USER}@ctit016.ewi.utwente.nl:${TARGET_FOLDER}/analysis/
# scp ./analysis/distribution_plot.py ${USER}@ctit016.ewi.utwente.nl:${TARGET_FOLDER}/analysis/
scp ./analysis/web_timeseries.py ${USER}@ctit016.ewi.utwente.nl:${TARGET_FOLDER}/analysis/
scp ./analysis/analysis_pipeline.py ${USER}@ctit016.ewi.utwente.nl:${TARGET_FOLDER}/analysis/
# scp ./loading/converter.py ${USER}@ctit016.ewi.utwente.nl:${TARGET_FOLDER}/loading/
scp ./util/parquet_file_to_pickle.py ${USER}@ctit016.ewi.utwente.nl:${TARGET_FOLDER}/util/
