import os
import configparser
import config.base_config as bc
from urllib.parse import urlparse
from utils.codelogger import basiclogger

comm_logger = basiclogger(logname=f'COMMON_ACTIONS', section=f'PROCESS', format_type=f'extended')
comm_except = basiclogger(logname=f'COMMON_ACTIONS', section=f'EXCEPTION', format_type=f'extended')


class CommonActions:
    """
    Some directory and string cleaning actions that are needed to carefully handle the MinIO requests
    especially concerning the prefix of an object needs appropriate treatment.
    """
    def __init__(self):
        comm_logger.info(f'Common Actions class gets initialised')

    @staticmethod
    def get_minio_conn_details(con_section=bc.aws_config_section) -> (str, str, str):
        """
        This is the section to check within the minio.config file headers and returns the respective values
        """
        try:
            comm_logger.info(f'Reading the config for: {bc.AWS_CONFIG}')
            read_con = configparser.ConfigParser()
            read_con.read(bc.AWS_CONFIG)

            host = read_con.get(con_section, f'host')
            parsed_host = urlparse(host).netloc or host
            aws_access_key_id = read_con.get(con_section, f'access_key')
            aws_secret_access_key = read_con.get(con_section, f'secret_key')
            comm_logger.info(f'The read host from config: {parsed_host}')

            return parsed_host, aws_access_key_id, aws_secret_access_key

        except Exception as e:
            comm_except.error(f"Issue with getting the values from config: {e}")
            raise f"Common Actions ConfigError: {e}"

    @staticmethod
    def split_minio_obj(obj_path):
        """
        This functions gets a minio prefix, without bucket name, and splits the base file with the extension
        """
        try:
            obj_name, obj_format = os.path.splitext(obj_path)
            comm_logger.info(f'The object name and format: {obj_name} & {obj_format}')
            return obj_name, obj_format

        except Exception as e:
            comm_except.error(f"Issue extracting the minio prefix and the file type from the given input: {e}")
            raise f"Common Actions MinIO split path exception: {e}"

    @staticmethod
    def get_full_object_path(obj_bucket, obj_path):
        """
        This method helps in forming the full minio object path from the given bucket and prefix names
        adding prefix 's3:// so this can be directly queried through duckDB'
        """
        try:
            full_minio_path = f"s3://{obj_bucket}/{obj_path}"
            comm_logger.info(f'The extracted full path: {full_minio_path}')
            return full_minio_path

        except Exception as e:
            comm_except.error(f"Exception with the minio paths used: {e}")
            raise f"Common Actions MinIO concat exception: {e}"

    @staticmethod
    def get_base_file_name(s3_path, with_extension=f'N'):
        """
        This method accepts the minio_prefix as an input parameter and returns the file name alone
        (without suffix/extension - type)
        """
        try:
            comm_logger.info(f'Is the input with an extension: {with_extension}')
            minio_file = os.path.basename(s3_path)
            comm_logger.info(f'The minio file: {minio_file}')
            if with_extension == f'Y':
                comm_logger.info(f'Returning the full file: {minio_file}')
                return minio_file
            else:
                base_name, _ = CommonActions.split_minio_obj(minio_file)
                comm_logger.info(f'Splitting the base file name alone: {base_name}')
                return base_name

        except Exception as e:
            print(f"Issue raised when getting the base file name: {e}")
            raise f"Common Actions Minio filename exception: {e}"

    @staticmethod
    def get_file_type(s3_path):
        """
        This method gets the s3 path (either full or prefix) and returns the type of the file, if its valid.
        returns None for directories
        """
        try:
            file_name, file_type = CommonActions.split_minio_obj(s3_path)
            file_type = file_type.lstrip(".")
            comm_logger.info(f'The file type for the given {s3_path} is "{file_type}"')
            return file_type

        except Exception as e:
            print(f"Issue with getting the file extension: {e}")
            raise f"Common Actions Minio file type exception: {e}"


if __name__ == f"__main__":
    genObj = CommonActions()
    r_host, access_key, secret_key = genObj.get_minio_conn_details(bc.aws_config_section)

    print(f"Details of minio!\nHost: {r_host}\nAccess: {access_key}\nSecret: {secret_key}")
