from .data_store import load_reviews_df, load_meta_df, save_processed_parquet

def main():
    print("Loading data...")
    df_reviews = load_reviews_df()
    df_meta = load_meta_df()

    print("Reviews:", df_reviews.shape)
    print("Meta:", df_meta.shape)
    print("Reviews columns sample:", df_reviews.columns.tolist()[:20])
    print("Meta columns sample:", df_meta.columns.tolist()[:20])

    print("Printing the first few columnns from reviews")
    print(df_reviews.head(2))
    print()

    print("Printing the first few columnns from meta")
    print(df_meta.head(2))
    print()

    print("Saving processed parquet")
    
    save_processed_parquet()

if __name__ == "__main__":
    main()