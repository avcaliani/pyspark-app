from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark \
        .read \
        .option('delimiter', ',') \
        .option('header', 'true') \
        .csv(path)


def read_mongo(spark: SparkSession, uri: str) -> DataFrame:
    return spark.read \
        .format('mongo') \
        .option('uri', uri) \
        .load()


def write_mongo(df: DataFrame, uri: str) -> None:
    df.write \
        .format('mongo') \
        .mode('append') \
        .option('uri', uri) \
        .save()
