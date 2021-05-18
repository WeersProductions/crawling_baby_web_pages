# Downloads the .pickle files from the cluster.

TARGET_FOLDER="WebInsight"
SCRIPT_LOCATION=$(dirname $0)
NODE_NUMBER="013"

echo "Downloading from "${TARGET_FOLDER}" to "${SCRIPT_LOCATION}

scp s1840495@ctit${NODE_NUMBER}.ewi.utwente.nl:${TARGET_FOLDER}/analysis/result/labelDistribution.pickle ${SCRIPT_LOCATION}
scp s1840495@ctit${NODE_NUMBER}.ewi.utwente.nl:${TARGET_FOLDER}/analysis/result/fetchDistribution.pickle ${SCRIPT_LOCATION}
scp s1840495@ctit${NODE_NUMBER}.ewi.utwente.nl:${TARGET_FOLDER}/analysis/result/diffExternalOutLinksDistribution.pickle ${SCRIPT_LOCATION}
scp s1840495@ctit${NODE_NUMBER}.ewi.utwente.nl:${TARGET_FOLDER}/analysis/result/diffInternalOutLinksDistribution.pickle ${SCRIPT_LOCATION}