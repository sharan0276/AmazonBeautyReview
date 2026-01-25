
import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, LongType

def main():
    parser = argparse.ArgumentParser(description="Stage 1 Dataset Curation for Early Product Success Prediction")
    parser.add_argument("--reviews", required=True, help="Path to input reviews parquet")
    parser.add_argument("--meta", required=True, help="Path to input metadata parquet")
    parser.add_argument("--out", required=True, help="Output directory for processed parquet files")
    args = parser.parse_args()

    # Initialize Spark Session (Local Mac Optimization)
    spark = SparkSession.builder \
        .appName("Stage1_Curation") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.caseSensitive", "true")
    print(f"Spark Config 'spark.sql.caseSensitive': {spark.conf.get('spark.sql.caseSensitive')}")

    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Loading reviews from: {args.reviews}")
    print(f"Loading metadata from: {args.meta}")

    # 1. Load Data
    print("Stage 1: Loading Data...")
    # Read parquet files. Schema is inferred but we expect certain columns based on requirements.
    try:
        # Hack: Force 'timestamp' to be read as LongType (it contains ms epoch)
        # Spark infers it as TimestampNTZ but values are large integers (ms), causing overflow/weird dates if interpreted directly.
        raw_schema = spark.read.option("mergeSchema", "true").parquet(args.reviews).schema
        new_fields = []
        for field in raw_schema.fields:
            if field.name == "timestamp":
                field.dataType = LongType()
            new_fields.append(field)
        final_schema = StructType(new_fields)
        
        df_reviews_raw = spark.read.schema(final_schema).parquet(args.reviews)
        df_meta_raw = spark.read.parquet(args.meta)
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # 2. Time Anchor & Launch Calculation
    # -------------------------------------------------------------------------
    print("Stage 2: Calculating Time Anchor & Launch Date...")
    # Modeling Grain: parent_asin
    
    # Convert timestamp (ms) to review_date (timestamp type)
    # We use cast("timestamp") which assumes the input is in seconds if using from_unixtime, 
    # but input is ms, so divide by 1000.
    # timestamp is treated as LongType now, so we can divide.
    
    df_reviews = df_reviews_raw.withColumn(
        "review_date", F.from_unixtime(F.col("timestamp") / 1_000_000_000).cast("timestamp")
    )
    
    # Compute launch_date per parent_asin as min(review_date)
    window_parent = Window.partitionBy("parent_asin")
    
    # We join launch_date back or use window function directly. 
    # For efficiency with large data, window function is fine if we persist or careful.
    # Let's add it as a column to the reviews.
    df_reviews = df_reviews.withColumn("launch_date", F.min("review_date").over(window_parent))
    
    # Compute day_from_launch
    df_reviews = df_reviews.withColumn(
        "day_from_launch", 
        F.datediff(F.col("review_date"), F.col("launch_date"))
    )

    # -------------------------------------------------------------------------
    # 3. Eligibility (Signal Quality)
    # -------------------------------------------------------------------------
    print("Stage 3: Checking Eligibility (Signal Quality)...")
    # reviews_28d: count reviews where 0 <= day_from_launch <= 27
    # eligible: reviews_28d >= 3
    # Rationale: early data is sparse, need sufficient signal.
    
    # We aggregate at parent_asin level to creating the product_index
    # First, flag reviews that fall in the window
    df_reviews = df_reviews.withColumn(
        "is_day_0_27", 
        F.when((F.col("day_from_launch") >= 0) & (F.col("day_from_launch") <= 27), 1).otherwise(0)
    )
    
    # -------------------------------------------------------------------------
    # 4. Observability (Avoid Right-Censoring)
    # -------------------------------------------------------------------------
    print("Stage 4: Checking Observability...")
    # global dataset end date
    dataset_end = df_reviews.agg(F.max("review_date")).collect()[0][0]
    print(f"Dataset End Date: {dataset_end}")
    
    # We will compute product level stats now
    df_product_stats = df_reviews.groupBy("parent_asin").agg(
        F.min("launch_date").alias("launch_date"),
        F.sum("is_day_0_27").alias("reviews_28d")
    )
    
    # Add Observability
    # Dataset End - 55 days
    cutoff_date = F.date_sub(F.lit(dataset_end), 55)
    
    df_product_stats = df_product_stats.withColumn(
        "observable_55d",
        F.col("launch_date") <= cutoff_date
    )
    
    # Add Eligibility
    df_product_stats = df_product_stats.withColumn(
        "eligible",
        F.col("reviews_28d") >= 3
    )
    
    # -------------------------------------------------------------------------
    # 5. Future Traction Flags
    # -------------------------------------------------------------------------
    print("Stage 5: Calculating Future Traction Flags...")
    # future_reviews_28d: count where 28 <= day_from_launch <= 55
    # Only meaningful for observable products, but we compute raw count first.
    
    df_reviews = df_reviews.withColumn(
        "is_future_28_55",
        F.when((F.col("day_from_launch") >= 28) & (F.col("day_from_launch") <= 55), 1).otherwise(0)
    )
    
    # We need to aggregate this back to product level. 
    # Since we already grouped, let's do a separate group and join, or do it all in one pass.
    # One pass aggregation is better. Re-doing aggregation with both metrics.
    
    df_product_agg = df_reviews.groupBy("parent_asin").agg(
        F.min("launch_date").alias("launch_date"),
        F.sum("is_day_0_27").alias("reviews_28d"),
        F.sum("is_future_28_55").alias("future_reviews_28d")
    )
    
    # Re-apply observability/eligibility logic on this aggregated DF
    df_product_index = df_product_agg.withColumn(
        "dataset_end", F.lit(dataset_end)
    ).withColumn(
        "observable_55d",
        F.col("launch_date") <= cutoff_date
    ).withColumn(
        "eligible",
        F.col("reviews_28d") >= 3
    )

    # Compute Flags (Only valid for observable)
    # traction_flag = 1 if future > 0
    # low_traction_flag = 1 if future == 0
    # Logic: If NOT observable, these should probably be null or irrelevant. 
    # Current requirement: "compute these only for observable_55d products" -> implying null otherwise or just logic applied there.
    # We will set them to null if not observable to avoid misuse, or 0. 
    # Requirement says "Create... compute these only for observable". 
    # We will use WHEN observable THEN compute ELSE null.
    
    df_product_index = df_product_index.withColumn(
        "traction_flag",
        F.when(F.col("observable_55d"), 
               F.when(F.col("future_reviews_28d") > 0, 1).otherwise(0)
        ).otherwise(None)
    ).withColumn(
        "low_traction_flag",
        F.when(F.col("observable_55d"),
               F.when(F.col("future_reviews_28d") == 0, 1).otherwise(0)
        ).otherwise(None)
    )
    
    # -------------------------------------------------------------------------
    # 6. Final Keep Rule & Filtering
    # -------------------------------------------------------------------------
    print("Stage 6: Applying Final Keep Rule & Filtering...")
    # keep_product = eligible AND observable_55d
    df_product_index = df_product_index.withColumn(
        "keep_product",
        F.col("eligible") & F.col("observable_55d")
    )
    
    # Add launch_year for convenience
    df_product_index = df_product_index.withColumn("launch_year", F.year("launch_date"))

    # Filtered Product List (small enough to broadcast if needed, but let's join normally)
    # We need to filter the main reviews dataset.
    
    df_kept_products = df_product_index.filter(F.col("keep_product") == True).select("parent_asin").distinct()
    
    # Filter Reviews
    # join with keeping rows
    df_reviews_filtered = df_reviews.join(df_kept_products, on="parent_asin", how="inner")
    
    # Select columns for reviews_filtered
    # parent_asin, asin, rating, text, title, helpful_vote, verified_purchase, timestamp
    # review_date, launch_date, day_from_launch
    # (user_id optional - retained only for EDA)
    cols_to_keep = [
        "parent_asin", "asin", "rating", "text", "title", "helpful_vote", "verified_purchase", "timestamp",
        "review_date", "launch_date", "day_from_launch", "user_id"
    ]
    # Select only available columns (some might be missing in raw if schema changes, but per req we assume they exist)
    # We intersect with available columns to be safe, or just select.
    available_cols = df_reviews_filtered.columns
    final_cols = [c for c in cols_to_keep if c in available_cols]
    df_reviews_filtered = df_reviews_filtered.select(*final_cols)

    # -------------------------------------------------------------------------
    # 7. Metadata Handling
    # -------------------------------------------------------------------------
    print("Stage 7: processing Metadata...")
    # meta_clean_filtered
    # Filter to kept products
    df_meta_filtered = df_meta_raw.join(df_kept_products, on="parent_asin", how="inner")
    
    # Drop media URL fields
    # Dropping bought_together: EDA showed 100% null coverage in All_Beauty metadata, 
    # so it adds no signal and only increases storage/processing cost.
    cols_to_drop = ["images", "videos", "bought_together"]
    df_meta_cleaned = df_meta_filtered.drop(*cols_to_drop)
    
    # Dropping categories: EDA found it is empty or not consistently populated for this subset, 
    # so it does not provide reliable enrichment for product-level modeling.
    cols_to_drop = ["categories"]
    df_meta_cleaned = df_meta_cleaned.drop(*cols_to_drop)

    # Note: We avoid generic "scan all columns to find 100% null" checks in Spark because it triggers
    # full-data actions (extra passes over parquet). Instead, we drop known low-value fields identified in EDA.


    # -------------------------------------------------------------------------
    # 8. QC & Observability Output
    # -------------------------------------------------------------------------
    print("Stage 8: Running Quality Checks...")
    # Caching product index for counts
    df_product_index.cache()
    
    total_parents = df_product_index.count()
    kept_parents = df_product_index.filter(F.col("keep_product") == True).count()
    
    print("\n" + "="*40)
    print("QUALITY CHECKS")
    print("="*40)
    print(f"Raw unique parent_asin: {total_parents}")
    print(f"Kept parent_asin: {kept_parents}")
    if total_parents > 0:
        print(f"% Kept: {kept_parents/total_parents*100:.2f}%")
    else:
        print("% Kept: 0.00%")
        
    # Check duplicates
    dup_check = df_product_index.groupBy("parent_asin").count().filter("count > 1").count()
    print(f"Duplicate parent_asin in index: {dup_check}")
    
    # Check integrity
    # confirm every parent_asin in reviews_filtered exists in product_index with keep_product=True
    # We rely on the join logic (inner join) which guarantees this by definition.
    print("Integrity check: reviews_filtered created via inner join on kept_products (Guaranteed)")

    # Distribution Summary
    # reviews_28d and future_reviews_28d
    print("\nDistribution Summary (Kept Products Only):")
    df_kept_stats = df_product_index.filter(F.col("keep_product") == True)
    df_kept_stats.select("reviews_28d", "future_reviews_28d").summary("min", "50%", "95%").show()

    # -------------------------------------------------------------------------
    # 9. Writing Output
    # -------------------------------------------------------------------------
    print("Stage 9: Writing Output...")
    print("Writing outputs...")
    
    # reviews_filtered
    out_reviews = f"{args.out}/reviews_filtered.parquet"
    df_reviews_filtered.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(out_reviews)
    print(f"Wrote {out_reviews}")
    
    # product_index
    out_index = f"{args.out}/product_index.parquet"
    df_product_index.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(out_index)
    print(f"Wrote {out_index}")
    
    # meta_clean_filtered
    out_meta = f"{args.out}/meta_clean_filtered.parquet"
    df_meta_cleaned.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(out_meta)
    print(f"Wrote {out_meta}")
    
    spark.stop()

if __name__ == "__main__":
    main()
