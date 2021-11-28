from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql import functions as F
from os import getenv

DATE_FORMAT = "yyyy-MM-dd"

dev: bool = True if getenv("STAGE", None) == "dev" else False
if dev:
    spark = (
        SparkSession.builder.master("local")
        .appName("Feature Engineering")
        .getOrCreate()
    )
    DATASET_PATH = "dataset_credit_risk.csv"
    FEATURE_PATH = "train_model_pyspark.csv"
else:
    spark = SparkSession.builder.appName("Feature Engineering").getOrCreate()
    DATASET_PATH = "s3a://kueski-ml-system/raw_data/2021/11/27/dataset_credit_risk.csv"
    FEATURE_PATH = "train_model_pyspark.csv"
Logger = spark._jvm.org.apache.log4j.Logger
logger = Logger.getLogger(__name__)
spark.sparkContext.setLogLevel("INFO")


def data_formatting(df: DataFrame) -> DataFrame:
    logger.info("FORMATTING DATAFRAME AND ORDERING")
    df = df.withColumn("loan_date", F.to_date("loan_date", DATE_FORMAT))
    df = df.withColumn("birthday", F.to_date("birthday", DATE_FORMAT))
    df = df.withColumn("job_start_date", F.to_date("job_start_date", DATE_FORMAT))
    return df.orderBy("id", "loan_date")


def feature_pipeline(df: DataFrame) -> DataFrame:
    logger.info("CREATING FEATURES")
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
    logger.info(f"READING CSV FROM {DATASET_PATH}")
    df = spark.read.option("header", True).option("inferSchema", True).csv(DATASET_PATH)
    df = data_formatting(df)
    df = feature_pipeline(df)

    df = df.select(
        "id",
        "age",
        "years_on_the_job",
        "nb_previous_loans",
        "avg_amount_loans_previous",
        "flag_own_car",
        "status",
    )

    df.show()
    df.printSchema()


    logger.info(f"WRITING CSV TO {FEATURE_PATH}")
    df.write.option("header", True).csv(FEATURE_PATH)
