import pandas as pd

def sample_csv(input_file: str, output_file: str, sample_size: int = 20):
    """Extracts a random sample of records from a large CSV file."""
    chunk_size = 10000  # Read in chunks to avoid memory issues
    sampled_df = pd.DataFrame()

    for chunk in pd.read_csv(input_file, chunksize=chunk_size):
        sampled_df = pd.concat([sampled_df, chunk.sample(min(len(chunk), sample_size))])
        if len(sampled_df) >= sample_size:
            break

    sampled_df.head(sample_size).to_csv(output_file, index=False)
    print(f"Sample saved to {output_file}")

if __name__ == "__main__":
    input_csv = "/home/writv/pyspark_etl/dataset/Books_rating.csv"  # Update with actual path
    output_csv = "sample.csv"
    sample_csv(input_csv, output_csv)
