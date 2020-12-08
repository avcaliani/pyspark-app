from pyspark.sql import SparkSession

from pipeline import PlayStorePipeline
from util import log


def spark_session() -> SparkSession:
    return SparkSession \
        .builder \
        .appName('pyspark-app') \
        .getOrCreate()


if __name__ == '__main__':
    spark = spark_session()
    log.info(f'Spark Version: {spark.version}')
    try:
        PlayStorePipeline(spark).run()
    except Exception as ex:
        log.error(f"Unexpected Error! {ex}")
    finally:
        spark.stop()
