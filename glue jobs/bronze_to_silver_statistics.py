import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, BooleanType
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime

print("STEP 1: Job setup...")
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "silver_bucket",
    "silver_database",
    "silver_table",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()
print("STEP 1 COMPLETE")

# ── Config ────────────────────────────────────────────────────────────────────
BRONZE_BASE   = "s3://yt-data-pipeline-bronze-ap-south2-dev/youtube/raw_statistics"
SILVER_BUCKET = args["silver_bucket"]
SILVER_DB     = args["silver_database"]
SILVER_TABLE  = args["silver_table"]
SILVER_PATH   = f"s3://{SILVER_BUCKET}/youtube/statistics/"
REGIONS       = ["ca", "gb", "us", "in", "de", "fr", "jp", "kr", "mx", "ru"]
print(f"STEP 2 COMPLETE: Silver={SILVER_PATH}")

# ── Step 3: Read each region separately and union ────────────────────────────
# WHY: Reading all regions from parent path causes Spark to auto-merge schemas
# across all 10 CSV files simultaneously. This fails because different region
# files have slight schema differences (column ordering, nulls, encoding).
# Reading each region individually and then union-ing gives us full control
# and avoids the schema conflict entirely.
print("STEP 3: Reading regions individually and combining...")

dfs = []
for region in REGIONS:
    path = f"{BRONZE_BASE}/region={region}/"
    try:
        df_region = spark.read \
            .option("header", "true") \
            .option("encoding", "ISO-8859-1") \
            .option("mode", "PERMISSIVE") \
            .option("escape", '"') \
            .option("quote", '"') \
            .option("maxCharsPerColumn", "65536") \
            .csv(path)

        # Add region column from path since we bypassed Glue catalog
        df_region = df_region.withColumn("region", F.lit(region))
        dfs.append(df_region)
        print(f"  Loaded region={region}")
    except Exception as e:
        print(f"  SKIPPED region={region}: {str(e)}")

# Union all region DataFrames into one
# unionByName handles any minor column ordering differences between files
df = dfs[0]
for df_next in dfs[1:]:
    df = df.unionByName(df_next, allowMissingColumns=True)

initial_count = df.count()
print(f"STEP 3 COMPLETE: Total rows={initial_count}")

if initial_count == 0:
    print("No records. Exiting.")
    job.commit()
else:
    # ── Step 4: Schema enforcement ────────────────────────────────────────────
    print("STEP 4: Casting types...")
    columns = set(df.columns)

    if "snippet.title" in columns or "snippet__title" in columns:
        print("Detected YouTube API format")
        df = df.select(
            F.col("id").alias("video_id"),
            F.lit(datetime.utcnow().strftime("%y.%d.%m")).alias("trending_date"),
            F.col("`snippet.title`").alias("title") if "snippet.title" in columns
                else F.col("snippet__title").alias("title"),
            F.col("`snippet.channelTitle`").alias("channel_title") if "snippet.channelTitle" in columns
                else F.col("snippet__channelTitle").alias("channel_title"),
            F.col("`snippet.categoryId`").cast(LongType()).alias("category_id") if "snippet.categoryId" in columns
                else F.col("snippet__categoryId").cast(LongType()).alias("category_id"),
            F.col("`snippet.publishedAt`").alias("publish_time") if "snippet.publishedAt" in columns
                else F.col("snippet__publishedAt").alias("publish_time"),
            F.col("`snippet.tags`").alias("tags") if "snippet.tags" in columns
                else F.lit(None).cast(StringType()).alias("tags"),
            F.col("`statistics.viewCount`").cast(LongType()).alias("views") if "statistics.viewCount" in columns
                else F.col("statistics__viewCount").cast(LongType()).alias("views"),
            F.col("`statistics.likeCount`").cast(LongType()).alias("likes") if "statistics.likeCount" in columns
                else F.col("statistics__likeCount").cast(LongType()).alias("likes"),
            F.col("`statistics.dislikeCount`").cast(LongType()).alias("dislikes") if "statistics.dislikeCount" in columns
                else F.lit(0).cast(LongType()).alias("dislikes"),
            F.col("`statistics.commentCount`").cast(LongType()).alias("comment_count") if "statistics.commentCount" in columns
                else F.col("statistics__commentCount").cast(LongType()).alias("comment_count"),
            F.col("`snippet.thumbnails.default.url`").alias("thumbnail_link") if "snippet.thumbnails.default.url" in columns
                else F.lit(None).cast(StringType()).alias("thumbnail_link"),
            F.lit(False).alias("comments_disabled"),
            F.lit(False).alias("ratings_disabled"),
            F.lit(False).alias("video_error_or_removed"),
            F.col("`snippet.description`").alias("description") if "snippet.description" in columns
                else F.col("snippet__description").alias("description"),
            F.col("region"),
        )
    else:
        print("Detected Kaggle CSV format")
        df = df.select(
            F.col("video_id").cast(StringType()),
            F.col("trending_date").cast(StringType()),
            F.col("title").cast(StringType()),
            F.col("channel_title").cast(StringType()),
            F.col("category_id").cast(LongType()),
            F.col("publish_time").cast(StringType()),
            F.col("tags").cast(StringType()),
            F.col("views").cast(LongType()),
            F.col("likes").cast(LongType()),
            F.col("dislikes").cast(LongType()),
            F.col("comment_count").cast(LongType()),
            F.col("thumbnail_link").cast(StringType()),
            F.col("comments_disabled").cast(BooleanType()),
            F.col("ratings_disabled").cast(BooleanType()),
            F.col("video_error_or_removed").cast(BooleanType()),
            F.col("description").cast(StringType()),
            F.col("region").cast(StringType()),
        )
    print("STEP 4 COMPLETE")

    # ── Step 5: Cleansing ─────────────────────────────────────────────────────
    print("STEP 5: Cleansing...")
    df = df.filter(F.col("video_id").isNotNull())
    df = df.withColumn("region", F.lower(F.trim(F.col("region"))))
    df = df.withColumn(
        "trending_date_parsed",
        F.when(
            F.col("trending_date").rlike(r"^\d{2}\.\d{2}\.\d{2}$"),
            F.to_date(F.col("trending_date"), "yy.dd.MM")
        ).otherwise(F.to_date(F.col("trending_date")))
    )
    numeric_cols = ["views", "likes", "dislikes", "comment_count"]
    for col_name in numeric_cols:
        df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))
    df = df.withColumn("like_ratio",
        F.when((F.col("views") > 0),
            F.round(F.col("likes") / F.col("views") * 100, 4)
        ).otherwise(0.0)
    )
    df = df.withColumn("engagement_rate",
        F.when((F.col("views") > 0),
            F.round((F.col("likes") + F.col("dislikes") + F.col("comment_count")) / F.col("views") * 100, 4)
        ).otherwise(0.0)
    )
    df = df.withColumn("_processed_at", F.current_timestamp())
    df = df.withColumn("_job_name", F.lit(args["JOB_NAME"]))
    print("STEP 5 COMPLETE")

    # ── Step 6: Deduplication ─────────────────────────────────────────────────
    print("STEP 6: Deduplicating...")
    from pyspark.sql.window import Window
    window = Window.partitionBy("video_id", "region", "trending_date_parsed") \
        .orderBy(F.col("_processed_at").desc())
    df = df.withColumn("_row_num", F.row_number().over(window)) \
        .filter(F.col("_row_num") == 1) \
        .drop("_row_num")
    clean_count = df.count()
    print(f"STEP 6 COMPLETE: clean_count={clean_count}")

    # ── Step 7: Write to Silver ───────────────────────────────────────────────
    print("STEP 7: Writing to Silver...")
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "silver_statistics")
    sink = glueContext.getSink(
        connection_type="s3",
        path=SILVER_PATH,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["region"],
        options={"write.mode": "overwrite"}
    )
    sink.setCatalogInfo(catalogDatabase=SILVER_DB, catalogTableName=SILVER_TABLE)
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(dynamic_frame)
    print(f"STEP 7 COMPLETE: {clean_count} records written")

job.commit()
print("JOB COMPLETE")