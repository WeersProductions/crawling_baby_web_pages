# Note: folders should be created on server side before this script runs.
# Format:
# Root
#   ^--- analysis
#   ^--- loading
#   ^--- util

USER="s1840495"
TARGET_FOLDER="WebInsight"

scp ./analysis/distribution_plot.py ${USER}@ctit016.ewi.utwente.nl:${TARGET_FOLDER}/analysis/
scp ./loading/converter.py ${USER}@ctit016.ewi.utwente.nl:${TARGET_FOLDER}/loading/
scp ./util/parquet_file_to_pickle.py ${USER}@ctit016.ewi.utwente.nl:${TARGET_FOLDER}/util/
