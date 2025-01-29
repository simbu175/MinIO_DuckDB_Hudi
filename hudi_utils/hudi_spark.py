from typing import Dict, Any

# import pyspark
# from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from config import base_config as bc
from hudi_utils.helper_functions import get_spark_config
from utils.minio_utils import MinUtils


# spark = SparkSession.builder \
#     .appName("HudiSetup") \
#     .config("spark.jars", bc.SPARK_CONF) \
#     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
#     .getOrCreate()


class HudiTableManager:
    def __init__(self, spark: SparkSession, hudi_options: Dict[str, Any]):
        self.spark = spark
        self.hudi_options = hudi_options

    def create_table(self, source_path: str, target_path: str):
        """
        Create a Hudi table by reading data from the source path and writing to the target path.
        """
        df = self.spark.read.csv(source_path, header=True, inferSchema=True)
        # df.show(3)
        # df.printSchema()
        df = df.withColumn("year", f.year("started_at"))
        p_keys = bc.HUDI_OPTIONS['hoodie.datasource.write.recordkey.field']
        df = df.dropDuplicates([p_keys])
        self._write_hudi(df, target_path, operation='insert_overwrite_table')

    def insert_data(self, source_path: str, target_path: str):
        """
        Insert some data to Hudi table by reading this from the source/input and writing to the target path.
        """
        df = self.spark.read.csv(source_path, header=True, inferSchema=True)
        df = df.withColumn("year", f.year("started_at"))
        self._write_hudi(df, target_path, operation='bulk_insert')

    def update_table(self, source_path: str, target_path: str):
        """
        Update a Hudi table by reading incremental data from the source path.
        """
        df = self.spark.read.csv(source_path, header=True, inferSchema=True)
        df = df.withColumn("year", f.year("started_at"))
        self._write_hudi(df, target_path, operation='upsert')

    def delete_records(self, delete_ids: DataFrame, target_path: str):
        """
        Delete records from a Hudi table using a DataFrame containing IDs to be deleted.
        """
        delete_options = self.hudi_options.copy()
        delete_options['hoodie.datasource.write.operation'] = 'delete'
        # delete_options['hoodie.datasource.write.precombine.field'] = 'started_at'

        delete_ids.write.format('hudi').options(**delete_options).mode('append').save(target_path)

    def _write_hudi(self, df: DataFrame, target_path: str, operation: str):
        """
        Helper method to write data to Hudi.
        """
        hudi_write_options = self.hudi_options.copy()
        hudi_write_options['hoodie.datasource.write.operation'] = operation  # 'insert_overwrite_table'

        if operation == f'insert_overwrite_table':
            df.write.format('hudi').options(**hudi_write_options).mode('overwrite').save(target_path)
        else:
            df.write.format('hudi').options(**hudi_write_options).mode('append').save(target_path)

    def read_hudi_table(self, target_path, query_type="snap", query_ts=None):
        if query_type == f"snap":
            return self.spark.read.format("hudi")\
                .option("hoodie.datasource.query.type", "snapshot")\
                .load(target_path)
        elif query_type == f"incr":
            return self.spark.read.format("hudi")\
                .option("hoodie.datasource.query.type", "incremental")\
                .option("hoodie.datasource.read.begin.instanttime", query_ts)\
                .load(target_path)
        else:
            return self.spark.read.format("hudi").load(target_path)


if __name__ == f"__main__":
    # set the spark and hudi configs along with MinIO
    spark_config = get_spark_config()
    # hudi_config = get_hudi_config(f'table')
    min_obj = MinUtils(config_section=bc.aws_config_section)

    s3a_endpoint = min_obj.parsed_host
    min_access_key = min_obj.aws_access_key_id
    min_secret_key = min_obj.aws_secret_access_key

    src_path = "s3a://raw-dataset/bikeshare/*.csv"
    src_upd_path = "s3a://raw-dataset/bikeshare/updates/*.csv"
    # tgt_path = "file:///D:/tmp/MinIO_Hudi/bikeshare"
    tgt_path = "s3a://compressed-zone/processed/hudi_db/bikeshare/"
    hudi_db = bc.HUDI_OPTIONS['hoodie.database.name']
    hudi_tbl = bc.HUDI_OPTIONS['hoodie.table.name']

    # Initialize the spark session
    spark_sess = SparkSession.builder.appName(f"BikeshareLoad")
    for key, value in spark_config.items():
        spark_sess = spark_sess.config(key, value)
    spark_sess = spark_sess.getOrCreate()
    spark_sess.sparkContext.setLogLevel(f"ERROR")

    hudi_options_cf = bc.HUDI_OPTIONS

    # read and write data to the Hudi table on top of MinIO buckets
    hudi_manager = HudiTableManager(spark_sess, hudi_options_cf)

    # hudi_manager.create_table(src_path, tgt_path)
    # hudi_manager.insert_data(table_name, new_data_path, table_base_path, table_config)
    # hudi_manager.update_table(src_upd_path, tgt_path)
    # delete_ids = spark.read.json("s3a://raw-data-bucket/delete_ids.json")
    # hudi_manager.delete_records(delete_ids, target_path)

    # print(f"Table before compaction! - 'F74DF9549B504A6B'")
    # hudi_df = hudi_manager.read_hudi_table(tgt_path, "N")
    # hudi_df.createOrReplaceTempView(f'{hudi_tbl}')
    # spark_sess.sql(f"SELECT * FROM {hudi_tbl} WHERE ride_id = 'F74DF9549B504A6B'").show()
    # running compaction to merge the delta log files on MOR to the base parquet file - hudi table name per hudi config
    spark_sess.sql(f" CREATE DATABASE IF NOT EXISTS {hudi_db} ")
    spark_sess.sql(f"""
        CREATE TABLE IF NOT EXISTS {hudi_db}.{hudi_tbl}
        USING hudi
        LOCATION '{tgt_path}'
    """)
    # print(f"CALL run_compaction(op => 'run', path => '{tgt_path}')")
    spark_sess.sql(f"CALL run_compaction(op => 'run', table => '{hudi_db}.{hudi_tbl}')")
    spark_sess.sql(f"CALL run_clean(table => '{hudi_db}.{hudi_tbl}', clean_policy => 'KEEP_LATEST_COMMITS')")
    # delete_marker
    # spark_sess.sql(f"CALL run_")
    print(f"Table after compaction! - 'F74DF9549B504A6B'")
    cp_df = hudi_manager.read_hudi_table(tgt_path, "time", "20250126162059363")
    cp_df.createOrReplaceTempView("bikeshare_comp")
    # spark_sess.sql(f"""SELECT * FROM bikeshare_comp WHERE ride_id = 'F74DF9549B504A6B' """).show()
    spark_sess.sql(f"SELECT * FROM bikeshare_comp WHERE ride_id = 'F74DF9549B504A6B' ").show()
    # spark_sess.sql(f"SHOW TABLES IN {hudi_db}").show()
