import pandas as pd


def main(input_file: str):
    print("Starting")
    df = pd.read_parquet(input_file, columns=['url', 'diffInternalOutLinks'])
    print(df.head())


if __name__ == "__main__":
    input_file = "./analysis/local/data/labeled_07-13_09-14.parquet"
    main(input_file)
