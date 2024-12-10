import os
import duckdb
# import configparser
import traceback
import config.base_config as bc
from utils.minio_utils import MinUtils
# from urllib.parse import urlparse
from utils.common_actions import CommonActions
from utils.codelogger import basiclogger

duck_logger = basiclogger(logname=f'DUCK_ESSENTIALS', section=f'PROCESS', format_type=f'extended')
duck_exceptions = basiclogger(logname=f'DUCK_ESSENTIALS', section=f'EXCEPTION', format_type=f'extended')


class DuckDB:
    """
    This class contains the Duckdb operations that are needed when connecting through Python, with duckdb module
    """
    def __init__(self, minio_host, minio_access_key, minio_secret_key, db_path=f":memory:"):
        self.minio_host = minio_host
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.db_path = db_path
        self.duckdb_dir = bc.DUCKDB_PATH
        self.con = None
        duck_logger.info(f'Duck DB utils class gets initialised')

    def connect(self):
        """
        Establishes a connection for the duckdb client
        """
        try:
            self.con = duckdb.connect(database=self.db_path, read_only=False)
            duck_logger.info("DuckDB connection established.")

            duck_logger.info("Loading the DuckDB with essentials to connect to MinIO")
            self.con.execute(f"""
                INSTALL httpfs; 
                LOAD httpfs;
                SET s3_endpoint='{self.minio_host}';
                SET s3_access_key_id='{self.minio_access_key}';
                SET s3_secret_access_key='{self.minio_secret_key}';
                SET s3_use_ssl=false; 
                SET s3_url_style='path';
            """)
            duck_logger.info("DuckDB configured for MinIO")
            return self.con

        except Exception as e:
            duck_exceptions.error(f"Error connecting to DuckDB: {e}")
            raise f'DUCKDB CREATE CONNECTION error: {e}'

    def execute_sql(self, _query_):
        """
        Executes the given query through the duckdb client
        """
        try:
            duck_logger.info(f"Executing DB query {_query_}")
            return self.con.sql(_query_)

        except Exception as e:
            duck_exceptions.error(f"There is an issue executing the SQL query: {e}")
            raise f'DUCKDB EXECUTE SQL exception: {e}'

    def execute(self, _query_, num_rows=1):
        """
        Executes the given query through the duckdb client
        """
        try:
            duck_logger.info(f"Executing DB query {_query_} with num_rows: {num_rows}")
            if not num_rows:
                return self.con.execute(_query_).fetchall()
            elif num_rows == 1:
                return self.con.execute(_query_).fetchone()
            else:
                return self.con.execute(_query_).fetchmany(size=num_rows)

        except Exception as e:
            duck_exceptions.error(f"There is an issue executing the given query: {e}")
            raise f'DUCKDB EXECUTE exception: {e}'

    def query_to_dataframe(self, _query_):
        """
        Executes a query and returns the result as a Pandas DataFrame.
        """
        try:
            df = self.con.execute(_query_).fetchdf()
            duck_logger.info(f"Query executed successfully and df is returned")
            return df

        except Exception as e:
            duck_exceptions.error(f"Error executing query: {e}")
            raise f'DUCKDB execute query to dataframe exception: {e}'

    def create_table_from_minio(self, table_name, s3_path):
        """
        Creates a table in DuckDB by reading a file from MinIO.
        """
        try:
            # _, file_ext = os.path.splitext(s3_path)
            # file_ext = file_ext.lstrip(".")
            file_ext = CommonActions.get_file_type(s3_path)
            self.con.execute(f"DROP TABLE IF EXISTS {table_name}; ")
            self.con.execute(f"CREATE TABLE {table_name} "
                             f"AS "
                             f"SELECT * FROM read_{file_ext}('{s3_path}'); ")
            duck_logger.info(f"Table '{table_name}' created successfully from {s3_path}.")

        except Exception as e:
            duck_exceptions.error(f"Error creating table from S3: {e}")
            raise f'DUCKDB TABLE CREATE exception: {e}'

    def write_table_to_minio(self, table_name, s3_path, file_format=f"parquet"):
        """
        Writes a DuckDB table to MinIO.
        """
        try:
            self.con.execute(f"COPY {table_name} TO '{s3_path}' (FORMAT '{file_format}')")
            duck_logger.info(f"Table '{table_name}' written successfully to {s3_path}.")

        except Exception as e:
            duck_exceptions.error(f"Error writing table to Minio: {e}")
            raise f'DUCKDB TABLE COPY exception: {e}'

    def convert_file_type(self, source_bucket, source_s3_path, target_bucket,
                          target_path=None, target_format=f"parquet"):
        """
        Converts a file from one format to another and writes it to MinIO.
        """
        # _, src_file_ext = os.path.splitext(source_s3_path)
        comm_obj = CommonActions
        _, src_file_ext = comm_obj.split_minio_obj(source_s3_path)
        # self.src_file_type = self.src_file_ext.lstrip(".")  # removed as we need the . from the previous step
        tgt_file_type = f".{target_format}"
        duck_logger.info(f'Get the base file path: {src_file_ext}')

        if src_file_ext:
            # src_path = f"s3://{source_bucket}/{source_s3_path}"
            src_path = comm_obj.get_full_object_path(source_bucket, source_s3_path)
            #  iteration for target path/key if available
            duck_logger.info(f'Source path: {src_path}')
            if not target_path:
                # tgt_path = f"s3://{target_bucket}/{source_s3_path.replace(src_file_ext, tgt_file_type)}"
                tgt_path = \
                    comm_obj.get_full_object_path(target_bucket, source_s3_path.replace(src_file_ext, tgt_file_type))
                duck_logger.info(f'Target full path: {tgt_path}')
            else:
                # can be done using a custom UDF
                # source_file = os.path.basename(source_s3_path)
                # base_name, _ = os.path.splitext(source_file)
                base_name = comm_obj.get_base_file_name(source_s3_path, 'N')
                tgt_file = f"{base_name}.{target_format}"
                duck_logger.info(f'Derived target file: {tgt_file}')
                # appending the base filename to the target key
                # tgt_path = f"s3://{target_bucket}/{os.path.join(target_path, tgt_file)}"
                tgt_path = comm_obj.get_full_object_path(target_bucket, os.path.join(target_path, tgt_file))
                duck_logger.info(f'Target full path: {tgt_path}')
            duck_logger.info(f"Source path: {src_path}\nTarget path: {tgt_path}")

            try:
                # DuckDB transformation query
                query_ = f""" COPY (SELECT * FROM '{src_path}') 
                TO '{tgt_path}' (FORMAT '{target_format}');
                """
                duck_logger.info(f'Query to convert: {query_}')
                self.con.execute(query_)
                duck_logger.info(f"Successfully converted {src_path} to {tgt_path}")

            except Exception as e:
                tgt_path = None
                duck_exceptions.error(traceback.format_exc(e))
                raise f'DUCKDB TYPE CONVERSION exception: {e}'

            return tgt_path

        else:
            tgt_path = None
            duck_logger.info(f"Invalid input key, not a file!")
            return tgt_path

    def export_db_to_disk(self, file_type=f'parquet'):
        """
        This method is to export the in-memory database to a file on disk
        """
        try:
            if not os.path.exists(self.duckdb_dir):
                duck_logger.info(f"If the directory doesn't exists, create '{self.duckdb_dir}'")
                os.makedirs(self.duckdb_dir)

            if file_type == f'csv':
                duck_logger.info(f'EXPORTING DATABASE in CSV... ')
                self.con.execute(f"EXPORT DATABASE '{self.duckdb_dir}'"
                                 f"(FORMAT CSV, DELIMITER '||' ) ;")
            elif file_type == f'parquet':
                duck_logger.info(f'EXPORTING DATABASE in parquet... ')
                self.con.execute(f"EXPORT DATABASE '{self.duckdb_dir}' "
                                 f"(FORMAT PARQUET, "
                                 f" COMPRESSION ZSTD, "
                                 f" ROW_GROUP_SIZE 100_000) ")
            #  need to implement the below EXPORT DB commands
            elif file_type == f'json':
                duck_logger.info(f'EXPORTING DATABASE in json... ')
            elif file_type == f'orc':
                duck_logger.info(f'EXPORTING DATABASE in orc... ')
            elif file_type == f'avro':
                duck_logger.info(f'EXPORTING DATABASE in avro... ')
            elif file_type == f'arrow':
                duck_logger.info(f'EXPORTING DATABASE in arrow... ')

            duck_logger.info(f"In-memory db successfully exported to {self.duckdb_dir}")

        except Exception as e:
            duck_exceptions.error(f"Exception occured when exporting duckdb to a local file: {e}")
            raise f'DUCKDB EXPORT DATABASE exception: {e}'

    def reload_db_from_disk(self):
        """
        This method is to import the db data from disk to in-memory
        """
        try:
            self.con.execute(f"IMPORT DATABASE '{self.duckdb_dir}';")
            duck_logger.info(f"In-memory db successfully imported from {self.duckdb_dir}")

        except Exception as e:
            duck_exceptions.error(f"Exception occured when importing duckdb from a local file: {e}")
            raise f'DUCKDB IMPORT DATABASE exception: {e}'

    def close_connection(self):
        """
        Closes the duckdb connection
        """
        try:
            if self.con:
                self.con.close()
                duck_logger.info(f"Duckdb connection closed successfully")
        except Exception as e:
            duck_exceptions.error(f'Exception occurred when closing {self.con}: {e}')
            raise f'DUCKDB CLOSE CONNECTION exception: {e}'


if __name__ == f"__main__":
    minio_obj = MinUtils(config_section=bc.aws_config_section)

    # minio_client = minio_obj.get_minio_client()
    # # List all CSV files in the source bucket
    objects = minio_obj.list_minio_objects(bucket_name=bc.s3_raw_bucket)

    duck_obj = DuckDB(minio_host=minio_obj.parsed_host,
                      minio_access_key=minio_obj.aws_access_key_id,
                      minio_secret_key=minio_obj.aws_secret_access_key)
    duck_con = duck_obj.connect()

    for obj in objects:
        duck_logger.info(f"Object details for {obj.object_name}: {obj}")

        # include only valid file types, csv, parquet, json, txt, orc
        if obj.object_name.endswith(bc.valid_file_formats):
            duck_logger.info(f"Processing file: {obj.object_name}")
            # output_obj = transform_csv_to_parquet(obj.object_name)
            output_obj = duck_obj.convert_file_type(source_bucket=bc.s3_raw_bucket,
                                                    source_s3_path=obj.object_name,
                                                    target_bucket=bc.s3_comp_bucket,
                                                    target_path=f'inputs/',
                                                    target_format='parquet')
            duck_logger.info(f"Transformed file: type({type(output_obj)}) - value: {output_obj}")
            query = f"SELECT * FROM '{output_obj}' LIMIT 1; "
            duck_logger.info(query)
            duck_logger.info(duck_obj.execute(_query_=query, num_rows=2))
            duck_logger.info(duck_obj.execute_sql(_query_=query))

        else:
            duck_logger.info(f"The retrieved object is not a valid "
                             f"file with any of these types: {bc.valid_file_formats}")

    duck_obj.export_db_to_disk()
    duck_obj.close_connection()
