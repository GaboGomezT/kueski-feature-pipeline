from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql import functions as F
import boto3
import botocore
import pandas as pd

from os import getenv

DATE_FORMAT = "yyyy-MM-dd"
BUCKET_NAME = 'kueski-ml-system' 
RISK_DATASET_KEY = 'raw_data/2021/11/27/dataset_credit_risk.csv'
FEATURES_KEY = 'feature_store/2021/11/28/train_model_pyspark.parquet.gzip'
DATASET = "dataset_credit_risk.csv"
FEATURE = "train_model_pyspark.parquet.gzip"

def download_file(bucket_name: str, file_key: str, file_local: str):
    s3 = boto3.resource('s3')
    try:
        s3.Bucket(bucket_name).download_file(file_key, file_local)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

def upload_file(bucket_name: str, file_key: str, file_local: str):
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_local, bucket_name, file_key)
    except botocore.exceptions.ClientError as e:
        print(f"Error: {e}")
        raise

dev: bool = True if getenv("STAGE", None) == "dev" else False
if dev:
    spark = (
        SparkSession.builder.master("local")
        .appName("Feature Engineering")
        .getOrCreate()
    )
else:
    spark = SparkSession.builder.appName("Feature Engineering").getOrCreate()
    download_file(BUCKET_NAME, RISK_DATASET_KEY, DATASET)
Logger = spark._jvm.org.apache.log4j.Logger
logger = Logger.getLogger(__name__)
# spark.sparkContext.setLogLevel("warn")


def data_formatting(df: DataFrame) -> DataFrame:
    logger.warn("FORMATTING DATAFRAME AND ORDERING")
    df = df.withColumn("loan_date", F.to_date("loan_date", DATE_FORMAT))
    df = df.withColumn("birthday", F.to_date("birthday", DATE_FORMAT))
    df = df.withColumn("job_start_date", F.to_date("job_start_date", DATE_FORMAT))
    return df.orderBy("id", "loan_date")


def feature_pipeline(df: DataFrame) -> DataFrame:
    logger.warn("CREATING FEATURES")
    w_avg = (
        Window.partitionBy("id")
        .orderBy(F.asc("loan_date"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)
    )
    w_rank = (
        Window.partitionBy("id")
        .orderBy(F.asc("loan_date"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (
        df.withColumn("avg_amount_loans_previous", F.avg("loan_amount").over(w_avg))
        .withColumn("nb_previous_loans", F.rank().over(w_rank) - 1)
        .withColumn("current_date", F.current_date())
        .withColumn("age", F.floor(F.datediff("current_date", "birthday") / F.lit(365)))
        .withColumn(
            "years_on_the_job",
            F.floor(F.datediff("current_date", "job_start_date") / F.lit(365)),
        )
        .withColumn(
            "years_on_the_job",
            F.when(F.col("years_on_the_job") < 0, None).otherwise(
                F.col("years_on_the_job")
            ),
        )
        .withColumn(
            "flag_own_car", F.when(F.col("flag_own_car") == "N", 0).otherwise(1)
        )
    )


if __name__ == "__main__":
    logger.warn(f"READING CSV FROM {DATASET}")
    df = spark.read.option("header", True).option("inferSchema", True).csv(DATASET)
    df = data_formatting(df)
    df = feature_pipeline(df)

    df = df.select(
        "id",
        "loan_date",
        "age",
        "years_on_the_job",
        "nb_previous_loans",
        "avg_amount_loans_previous",
        "flag_own_car",
        "status",
    )

    df.show()
    df.printSchema()


    logger.warn(f"WRITING CSV TO {FEATURE}")
    pandas_df = df.toPandas()
    pandas_df["loan_date"] = pd.to_datetime(pandas_df["loan_date"])
    pandas_df.to_parquet(FEATURE, compression='gzip')
    if not dev:
        logger.warn(f"UPLOADING FILE TO S3 {BUCKET_NAME}/{FEATURES_KEY}")
        upload_file(BUCKET_NAME, FEATURES_KEY, FEATURE)
