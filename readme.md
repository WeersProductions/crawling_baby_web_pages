# Crawling baby web pages


## Structure
### loading/
Loads the raw `.json` files and converts them into `.parquet` files. Selects and computes some properties. Schema can be found in `loading/converter.py`.

### analysis/
Contains code to analyze the processed `.parquet` files.
`distribution_plot.py` will join on a label <todo/>.

## Developer work flow
Edit files in the repository.
Copy them to the cluster using the script: `upload_to_cluster.sh`. This will copy all required scripts to the cluster.
### MacOS
```
chmod +x upload_to_cluster.sh
./upload_to_cluster.sh
```

Each file contains an example of how to run the file using spark.

## Data heads
Heads of data can be found in data-preview.md. All URLS are removed and replaced with `[A CENSORED WEBSITE URL]`. Matched using: `(http|https)?:\/\/(\S+)"`.
