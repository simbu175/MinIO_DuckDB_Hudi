# BASE_PATH = os.environ['PROJECT_PATH']
BASE_PATH = f"D:\\Personal Projects\\MinIO_Hudi\\MinIO_DuckDB_Hudi"
AWS_CONFIG = BASE_PATH + f"\\config\\minio.config"
DUCKDB_PATH = f"D:\\Personal Projects\\Duckdb-Data"
DATA_PATH = f"D:\\Personal Projects\\Ext datasets\\Kaggle\\F1\\1950-2024\\"
SPARK_JAR = f"D:\\Apps\\Hudi\\Jars\\hudi-spark3.5-bundle_2.12-1.0.0.jar"
LOGGER_FILE = BASE_PATH + f"\\config\\default_logger.config"
LOGFOLDEFR = BASE_PATH + f"\\logs"
SPARK_CONF = BASE_PATH + f"\\config\\spark_config.json"
# expects Mode environment variable to be created prior usage
aws_config_section = f"DEVELOPER"
s3_raw_bucket = f"raw-dataset"
s3_comp_bucket = f"compressed-zone"
s3_default_region = f"us-east-1"  # default region value
s3_raw_input_object = f"input/"
# local_source_file = r"D:\Personal Projects\Ext datasets\Kaggle\F1\1950-2024"
valid_file_formats = ('csv', 'parquet', 'json', 'txt', 'orc', 'gz', 'avro',)

hudi_tbl_config = ["hoodie.table.name", "hoodie.datasource.write.recordkey.field",
                   "hoodie.datasource.write.partitionpath.field", "hoodie.datasource.write.table.type",
                   "hoodie.datasource.write.precombine.field", "hoodie.datasource.hive_sync.enable",
                   "hoodie.datasource.hive_sync.database", "hoodie.datasource.hive_sync.table"]

spark_job_properties = ["spark.serializer", "spark.sql.catalogImplementation", "spark.jars", "spark.sql.extensions",
                        "spark.executor.memory"]

HUDI_OPTIONS = {
    "hoodie.table.name": "bikeshare",  # table name for HUDI
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",  # "MERGE_ON_READ", COPY_ON_WRITE
    "hoodie.datasource.write.recordkey.field": "ride_id",  # primary key
    "hoodie.datasource.write.precombine.field": "started_at",
    "hoodie.database.name": "hudi_db",
    "hoodie.write.concurrency.mode": "OPTIMISTIC_CONCURRENCY_CONTROL",  # "NON_BLOCKING_CONCURRENCY_CONTROL",
    "hoodie.datasource.write.partitionpath.field": "year",  # partition field column, must present in table schema
    "hoodie.datasource.write.operation": "upsert",  # append or upsert for incremental MOR table types
    "hoodie.parquet.max.file.size": "26214400",  # 10MB - 10485760
    "hoodie.parquet.block.size": "26214400",  # 25MB - 26214400
    "hoodie.copyonwrite.record.size.estimate": "512",
    "hoodie.parquet.compression.codec": "snappy",
    "hoodie.clean.automatic": "true",
    "hoodie.clean.policy": "KEEP_LATEST_FILE_VERSIONS",  # keeping the latest file - KEEP_LATEST_COMMITS
    "hoodie.cleaner.commits.retained": "1",  # for time travel, how many commits to go back to
    "hoodie.compact.inline": "false",  # inline compaction disabling to show delta log files
    "hoodie.metadata.enable": "true",  # Ensure Metadata Table is enabled for optimizing
    "hoodie.index.type": "RECORD_INDEX",  # GLOBAL_BLOOM, RECORD_INDEX, BUCKET, GLOBAL_SIMPLE
    "hoodie.metadata.record.index.enable": "true",  # record level lookup - exact row lookup
    "hoodie.record.index.update.partition.path": "RECORD_INDEX_UPDATE_PARTITION_PATH_ENABLE",  # updates handling
    "hoodie.metadata.column.stats.enable": "true",  # enabling column level stats
    "hoodie.metadata.partition.stats.enable": "true",  # enable partition stats index
    "hoodie.bloom.index.enable": "true",  # bloom filter
    "hoodie.metadata.expression.index.enable": "true",  # expression index for queries involving expressions
}
