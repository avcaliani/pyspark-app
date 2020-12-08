
from os import environ as env
from pyspark.sql import DataFrame, functions as f
from pyspark.sql.session import SparkSession

from util import log

TAG = 'Play Store'
DATALAKE_PATH = env.get('DATALAKE_PATH', '/datalake')


class PlayStorePipeline:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @property
    def raw_path(self):
        return f'{DATALAKE_PATH}/raw/play-store'

    def run(self) -> None:
        log.info(f'{TAG}: STARTED')
        df = self.read()
        log.info(f'{TAG}: {df.count()} records found!')
        log.info(f'{TAG}: PROCESSING')
        self.write(self.process(df))
        log.info(f'{TAG}: FINISHED')

    def read(self) -> DataFrame:
        return self.spark \
            .read \
            .option('delimiter', ',') \
            .option('header', 'true') \
            .csv(self.raw_path)

    def write(self, df: DataFrame) -> None:
        # TODO: Write to Mongo
        df.printSchema()
        df.count()
        df.show()

    def process(self, df: DataFrame) -> DataFrame:
        df = self.rename_cols(df)
        df = self.parse_cols(df)
        df = self.data_quality(df)
        return df

    @classmethod
    def rename_cols(cls, df: DataFrame) -> DataFrame:
        new_names = map(
            lambda c: f.col(c).alias(str(c).strip().replace(' ', '_').lower()),
            df.columns
        )
        return df.select(*list(new_names))

    @classmethod
    def parse_cols(cls, df: DataFrame) -> DataFrame:
        return df \
            .withColumn('category', f.upper(f.col('category'))) \
            .withColumn('rating', f.col('rating').cast('double')) \
            .withColumn('reviews', f.col('reviews').cast('long')) \
            .withColumn('installs', f.regexp_replace(f.col('installs'), r'\D', '').cast('long')) \
            .withColumn('type', f.upper(f.col('type'))) \
            .withColumn('price', f.regexp_replace(f.col('price'), r'\$', '').cast('double')) \
            .withColumn('genres', f.split(f.col('genres'), ';')) \
            .withColumn('last_updated', f.to_date(f.col('last_updated'), 'MMMM d, yyyy'))

    @classmethod
    def data_quality(cls, df: DataFrame) -> DataFrame:
        return df \
            .filter(~f.col('last_updated').isNull() & ~f.col('android_ver').isNull()) \
            .fillna({'rating': 0.0, 'reviews': 0, 'installs': 0, 'price': 0.0})