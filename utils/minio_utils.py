# import sys
# import configparser
import os
import glob
import re

# from urllib.parse import urlparse
from minio import Minio
from config import base_config as bc
from utils.progress import Progress
from utils.common_actions import CommonActions
from utils.codelogger import basiclogger

minio_logger = basiclogger(logname=f'MINIO_ESSENTIALS', section=f'PROCESS', format_type=f'extended')
minio_except = basiclogger(logname=f'MINIO_ESSENTIALS', section=f'EXCEPTION', format_type=f'extended')


class MinIO:
    """
    This class is used only for defining the MinIO host connection parameters
    and returning with a client to interact programmatically with the MinIO through its APIs
    """

    def __init__(self, config_section=bc.aws_config_section):
        # aws_con = configparser.ConfigParser()
        # aws_con.read(bc.AWS_CONFIG)
        # self.host = aws_con.get(config_section, f'host')
        # self.parsed_host = urlparse(self.host).netloc or self.host
        # self.aws_access_key_id = aws_con.get(config_section, f'access_key')
        # self.aws_secret_access_key = aws_con.get(config_section, f'secret_key')
        comm_obj = CommonActions()
        minio_logger.info(f'Initializing the MinIO class!!!')
        self.parsed_host, self.aws_access_key_id, self.aws_secret_access_key = \
            comm_obj.get_minio_conn_details(config_section)
        minio_logger.info(f'Initialized the MinIO host as {self.parsed_host}')

    def get_minio_client(self):
        """
        Defines the minio client for the programmatic access to its features
        """
        try:
            minio_client_connect = Minio(
                self.parsed_host,
                access_key=self.aws_access_key_id,
                secret_key=self.aws_secret_access_key,
                secure=False
            )
            minio_logger.info(f'Defined MinIO API set at {minio_client_connect}!!!')
            return minio_client_connect

        except Exception as e:
            minio_except.error(f'Exception occured when connecting to the minio console: {e}')
            raise f'MinIO client connect Exception: {e}'


class MinUtils(MinIO):
    """
    # can do put_object and fput_object
    This class has the essential methods you need for using MinIO like
    creating a bucket, removing a bucket, getting the list of buckets and objects in a bucket,
    removing an object, etc.
    """

    def __init__(self, config_section, target_bucket_name=None,
                 content_type=f'application/csv', metadata=None, sse=None, part_size=0,
                 num_parallel_uploads=10, tags=None, retention=None, legal_hold=None):
        super().__init__(config_section)
        self.bucket_name = target_bucket_name
        # self.target_object = target_object
        # self.full_path = input_file_path
        self.content_type = content_type
        self.metadata = metadata
        self.sse = sse
        self.part_size = part_size
        self.num_parallel_uploads = num_parallel_uploads
        self.tags = tags
        self.retention = retention
        self.legal_hold = legal_hold

    def write_into_minio(self, bucket_name, minio_path, local_file):
        # bucket_name, object_name, file_path, content_type=”application/octet-stream”, metadata=None, sse=None,
        # progress=None, part_size=0, num_parallel_uploads=3, tags=None, retention=None, legal_hold=False
        """
        This method is to write the local file to the remote bucket in MinIO
        :param bucket_name: The name of the bucket for which you need to put the data into
        :param minio_path: The prefix (in s3 standards) that the object should be inside the bucket
        :param local_file: The actual file with the extension that gets uploaded to the path shared above
        :return: None
        """
        try:
            progress = Progress()
            self.get_minio_client().fput_object(bucket_name=bucket_name,
                                                object_name=minio_path,
                                                file_path=local_file,
                                                content_type=self.content_type,
                                                metadata=self.metadata,
                                                sse=self.sse,
                                                progress=progress,
                                                part_size=self.part_size,
                                                num_parallel_uploads=self.num_parallel_uploads,
                                                tags=self.tags,
                                                retention=self.retention,
                                                legal_hold=self.legal_hold
                                                )
            minio_logger.info(f'The input file {local_file} is successfully '
                              f'written to the path s3://{bucket_name}/{minio_path}')

        except Exception as e:
            minio_except.error(f'Exception occurred when writing data to MinIO bucket: {e}')
            raise f'MinIO write exception for local file: {e}'

    def load_file_paths(self, local_path, bucket_name, minio_path):
        """
        The method extracts the metadata of the given local file and recursively iterates all the files
        in case, if we're uploading entire folder to the minIO bucket. And for each file inside, it iteratively
        uploads them to the MinIO bucket using the earlier function defined above
        :param local_path: The local path (or the folder path, in case) or the file path from Windows, say
        :param bucket_name: The MinIO bucket where you want to upload to
        :param minio_path: The prefix inside the bucket where you want the files to get uploaded to
        :return: A string to notify the operation status: 'Success' or 'Failed'
        """
        try:
            if os.path.isdir(local_path):
                minio_logger.info(f"Since the given path '{local_path}' is a directory, "
                                  f"iterating through all the files")
                for local_file in glob.glob(local_path + f'/**'):
                    minio_logger.info(f'Converting the windows path to standard minio dir path: {local_file}')
                    local_file = local_file.replace(os.sep, f'/')  # Replace \ with / on Windows
                    minio_logger.info(f'Local file to upload: {local_file}')
                    if not os.path.isfile(local_file):
                        minio_logger.info(f'If "{local_file}" is not a file, '
                                          f'iterate it through the folder by calling the same function recursively')
                        self.load_file_paths(local_file, bucket_name, minio_path + '/' + os.path.basename(local_file))

                    else:
                        #  local_file[1 + len(local_path):]) -- use this in the place of os.path.basename for issues
                        minio_logger.info(f'If "{local_file}" is a file, '
                                          f'then directly upload it to the MinIO bucket using the above function')
                        remote_path = os.path.join(minio_path, os.path.basename(local_file))
                        remote_path = remote_path.replace(os.sep, f'/')  # Replace \ with / on Windows
                        minio_logger.info(f'Remote path to write to: {remote_path}')
                        self.write_into_minio(self.bucket_name, remote_path, local_file)

            else:
                output_min_path = os.path.join(minio_path, os.path.basename(local_path)).replace(os.sep, f'/')
                minio_logger.info(f'Uploading "{local_path}" to: {output_min_path}')
                self.write_into_minio(self.bucket_name,
                                      output_min_path,
                                      local_path)

            minio_logger.info(f'Upload success!!')
            return 'Success'

        except Exception as e:
            minio_except.error(f'Exception occurred when uploading files: {e}')
            raise f'MinIO write data exception: {e}'

    def list_minio_objects(self, bucket_name=bc.s3_raw_bucket, prefix=None,
                           recurse=True, include_user_meta_data=True,
                           start_after=None, include_version=None):
        """
        The method helps in listing the objects inside the given MinIO bucket with additional details
        :param bucket_name: Bucket name in which the listing needs to be done
        :param prefix: The folder/directory inside the bucket for listing (as optional)
        :param recurse: Iterate through recursively for all the files inside
        :param include_user_meta_data: Include the user metadata like owner_id and owner_name, etc
        :param start_after: A particular key to start after (either alphabetically or chronologically)
        :param include_version: Specify any versions of the data in case, if versioning is enabled
        :return: The object iterator that is returned by the list_objects API
        """
        try:
            objs = self.get_minio_client().list_objects(bucket_name=bucket_name,
                                                           prefix=prefix,
                                                           recursive=recurse,
                                                           start_after=start_after,
                                                           include_user_meta=include_user_meta_data,
                                                           include_version=include_version)
            # objects -> iterator of the type
            minio_logger.info(f'Successfully retrieved the objects: {objs}')
            return objs

        except Exception as e:
            minio_except.error(f"Exception occurred when listing objects: {e}")
            raise f'MinIO List object exception: {e}'

    def is_empty_object(self, bucket_name=bc.s3_raw_bucket, obj_name=None,  # ssec=None,
                        # version_id=None, extra_headers=None, extra_query_params=None
                        ):
        """
        This method is used to check if an object/prefix exists in MinIO. If an exception is raised,
        then it means the object doesn't exist in MinIO. For an existing object, it lists the metadata of the object.
        :param bucket_name: Name of the bucket
        :param obj_name: Prefix/key in the bucket that needs to be checked for emptiness
        :return: True or False, based on the existence of the object in the MinIO bucket
        """
        try:
            minio_logger.info(f"Searching for the key: {obj_name} in bucket: {bucket_name}")
            obj = self.list_minio_objects(bucket_name=bucket_name, prefix=obj_name, recurse=False)
            for _ in obj:
                minio_logger.info(f"A file exists in the key shared: {_.object_name}. Hence its not empty")
                return True
            return False

        except Exception as e:
            print(f"Exception: {e}")
            minio_except.error(f"The {obj_name} doesn't exist in the MinIO bucket {bucket_name}")
            return False

    @staticmethod
    def is_valid_bucket_name(b_name):
        """
        Checks the given string is a valid bucket name as per the standards below.
        1. Length of the bucket name - between 3 and 63
        2. Can contain only lower case characters, numbers, periods (.) and dashes (-)
        3. Can not contain "_" in the name, or end with a (-) and have double periods (..)
        and dashes adjacent to periods (.-)
        4. Name can not be in the IP address form
        :param b_name: name of the bucket to do the validations. Return true only if the above criteria passes
        :return: Returns the validation status of the given name for a bucket name considerations
        """
        try:
            val_result = f"Failed!"
            # Rule 1: Check for the length of the bucket name
            if not 3 < len(b_name) < 63:
                val_result += f"\nBucket name must be between 3 and 63 characters."

            # Rule 2: Valid character check - lowercase, numbers, periods, dashes
            if not re.match(r'^[a-z0-9.-]+$', b_name):
                val_result += f"\nBucket name should contain only lowercase, numbers, periods (.), dashes (-)"

            # Rule 3: Invalid patterns to block
            if "_" in b_name:
                val_result += f"\nBucket name cannot contain underscore (_)"
            if b_name.endswith("-"):
                val_result += f"\nBucket name cannot end with dash (-)"
            if ".." in b_name:
                val_result += f"\nBucket name cannot contain double periods (..)"
            if ".-" in b_name or "-." in b_name:
                val_result += f"\nBucket name cannot have dashes and periods next to each other"

            # Rule 4: Can not be in IP address format
            ip_pattern = r"^(\d{1,3}\.){3}\d{1,3}$"
            if re.match(ip_pattern, b_name):
                val_result += f"\nBucket name cannot be in IP address format"

            val_result = "Success" if val_result == f"Failed!" else val_result
            minio_logger.info(f"Validation result: {val_result}")
            return val_result

        except Exception as e:
            minio_except.error(f"Exception occured when validating the bucket name: {e}")
            return False

    def create_minio_bucket(self, bucket_name, region=bc.s3_default_region, obj_lock=True):
        """
        This method is used to create a new MinIO bucket, for the region specified. Default region otherwise
        :param obj_lock: Flag to set the object-lock feature
        :param region: Default region: us-east-1, if not specified otherwise
        :param bucket_name: Name for the bucket to be created already
        :return: String, usually a success message or a warning/failure text
        """
        try:
            minio_logger.info(f"Check if the bucket: {bucket_name} already exists before creating a new one")
            if self.get_minio_client().bucket_exists(bucket_name=bucket_name):
                minio_logger.info(f"The bucket - {bucket_name} already exists in MinIO so not creating this again!!")
                return f"Bucket {bucket_name} already exists in {bc.s3_default_region} location"

            else:
                if self.is_valid_bucket_name(b_name=bucket_name) == f"Success":
                    min_bucket = self.get_minio_client().make_bucket(bucket_name=bucket_name, location=region,
                                                                     object_lock=obj_lock)
                    minio_logger.info(f"The bucket {bucket_name} is successfully created !!")
                    return min_bucket
                else:
                    return "Invalid bucket name!"

        except Exception as e:
            minio_except.error(f"An exception occured when creating the bucket: {e}")

    def delete_minio_bucket(self, bucket_name, ):
        """
        Method to remove an existing bucket. Checks if the bucket is already existing and then proceeds to remove.
        :param bucket_name: Name of the bucket to remove
        :return: Bool: True or False
        """
        try:
            if self.get_minio_client().bucket_exists(bucket_name=bucket_name):
                self.get_minio_client().remove_bucket(bucket_name=bucket_name)
                minio_logger.info(f"Bucket - {bucket_name} successfully removed")
            else:
                minio_logger.info(f"Bucket - {bucket_name} doesn't exist in MinIO")
        except Exception as e:
            minio_except.error(f"An exception occured when deleting the minio bucket: {e}")

    def delete_minio_object(self):
        pass


if __name__ == f"__main__":
    s3_obj = MinUtils(config_section=bc.aws_config_section,
                      target_bucket_name=bc.s3_raw_bucket,
                      )
    # print(f" Get the minio client: {s3_obj.get_minio_client()}")
    minio_logger.info(f" Get the minio client: {s3_obj.get_minio_client()}")

    # output_data = s3_obj.load_file_paths(
    #     local_path=bc.DATA_PATH,
    #     bucket_name=bc.s3_raw_bucket,
    #     minio_path=f'inputs')
    # minio_logger.info(f"\nFile status: {output_data}")

    # objects = s3_obj.list_minio_objects(bucket_name=bc.s3_comp_bucket)
    # for i in objects:
    #     print(i.bucket_name, i.object_name, i.last_modified)

    check_path = f"input"  # 'inputs/sprint_results.parquet'
    if s3_obj.is_empty_object(bucket_name=bc.s3_comp_bucket, obj_name=check_path):
        print(f"The path {check_path} is not empty in {bc.s3_comp_bucket}")
    else:
        print(f"This is an empty object/prefix or is yet to be created")

    # s3_obj.create_minio_bucket(f"test-bucket")
    # s3_obj.delete_minio_bucket(f"test-bucket")
    # val = s3_obj.is_valid_bucket_name(f"a" * 64)
    # print(f"Validation result: {val}")
