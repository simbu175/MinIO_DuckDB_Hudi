# BASE_PATH = os.environ['PROJECT_PATH']
BASE_PATH = f"D:\\Personal Projects\\f1"
# AWS_CONFIG = BASE_PATH + f"/Lake_Ingestion_Codes/config/aws.config"
AWS_CONFIG = BASE_PATH + f"\\config\\minio.config"
DUCKDB_PATH = f"D:\\Personal Projects\\Duckdb-Data"
DATA_PATH = f"D:\\Personal Projects\\Ext datasets\\Kaggle\\F1\\1950-2024\\"

# expects Mode environment variable to be created prior usage
aws_config_section = f"DEVELOPER"
s3_raw_bucket = f"raw-dataset"
s3_comp_bucket = f"compressed-zone"
s3_raw_input_object = f"input/"
# local_source_file = r"D:\Personal Projects\Ext datasets\Kaggle\F1\1950-2024"
valid_file_formats = ('csv', 'parquet', 'json', 'txt', 'orc', 'gz', 'avro', )
