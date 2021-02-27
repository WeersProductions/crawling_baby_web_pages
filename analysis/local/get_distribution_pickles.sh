# Downloads the .pickle files from the cluster.

TARGET_FOLDER="WebInsight"
SCRIPT_LOCATION=$(dirname $0)

echo "Downloading from "${TARGET_FOLDER}" to "${SCRIPT_LOCATION}

scp s1840495@ctit016.ewi.utwente.nl:${TARGET_FOLDER}/analysis/result/labelDistribution.pickle ${SCRIPT_LOCATION}
scp s1840495@ctit016.ewi.utwente.nl:${TARGET_FOLDER}/analysis/result/fetchDistribution.pickle ${SCRIPT_LOCATION}