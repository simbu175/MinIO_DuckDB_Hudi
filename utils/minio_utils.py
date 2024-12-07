# import sys
# import configparser
import os
import glob

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

    def create_minio_bucket(self, bucket_name, region, object_lock=True):
        """
        This method is for creating buckets within MinIO storage
        :param bucket_name: name of the bucket to be created
        :param region: applicable for cloud storage to specify the region of the cloud
        :param object_lock: Object locking is applicable for versioned buckets,
        where you could write once and read many times
        :return: None
        """
        try:
            minio_logger.info(f"Going to create a MinIO bucket: {bucket_name}")
            self.get_minio_client().make_bucket(bucket_name=bucket_name,
                                                location=region,
                                                object_lock=object_lock)

        except Exception as e:
            minio_except.error(f"Exception occured when creating bucket: {e}")

    def list_all_buckets(self):
        """
        This method is to list all the buckets and return the bucket object iterator that can be
        iterated later on for bucket name and bucket created date etc.
        :return: Returns the bucket object iterator
        """
        try:
            minio_logger.info(f"Listing all the buckets.. ")
            buckets = self.get_minio_client().list_buckets()
            return buckets

        except Exception as e:
            minio_except.error(f"Exception occured when listing MinIO buckets: {e}")

    def delete_minio_bucket(self, bucket_name):
        """
        The method deletes the bucket from MinIO storage when given the bucket_name as input
        :param bucket_name: The MinIO bucket to remove
        :return: None
        """
        try:
            minio_logger.info(f"Check if the given bucket {bucket_name} exists already?")
            if self.get_minio_client().bucket_exists(bucket_name=bucket_name):
                minio_logger.info(f"Proceeding to delete as it exists.. ")
                self.get_minio_client().remove_bucket(bucket_name=bucket_name)

        except Exception as e:
            minio_except.error(f"Exception occured when removing the bucket: {e}")

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
            objects = self.get_minio_client().\
                list_objects(bucket_name=bucket_name,
                             prefix=prefix,
                             recursive=recurse,
                             start_after=start_after,
                             include_user_meta=include_user_meta_data,
                             include_version=include_version)
            # objects -> iterator of the type
            minio_logger.info(f'Successfully retrieved the objects: {objects}')
            return objects

        except Exception as e:
            minio_except.error(f"Exception occurred when listing objects: {e}")
            raise f'MinIO List object exception: {e}'

    def delete_minio_object(self, bucket_name, object_name, version_id=None):
        """
        Method to remove the given MinIO object in the bucket
        :param bucket_name: Bucket name in which the underlying object lives
        :param object_name: The object under the bucket that needs deletion
        :param version_id: Version ID, applicable only for versioned buckets
        :return: None
        """
        try:
            minio_logger.info(f"Removing the given object: {object_name}")
            self.get_minio_client().remove_object(bucket_name=bucket_name,
                                                  object_name=object_name,
                                                  version_id=version_id)

        except Exception as e:
            minio_except.error(f"Exception occurred when removing the given object: {e}")

    def delete_minio_objects(self, bucket_name, object_list, bypass_governance_mode=False):
        """
        Method to delete multiple objects for a given bucket. Ensure to give the object paths as list of
        object_names so this can be deleted through the DeleteObject iterator
        :param bucket_name: MinIO bucket name
        :param object_list: List of objects inside the bucket that needs deletion with a single API call
        :param bypass_governance_mode: False by default, to remove the s3:BypassGovernanceRetention permission
        on the bucket so that MinIO can lift the lock automatically
        :return: An error iterator for DeleteError object
        """
        try:
            minio_logger.info(f"Deleting the objects, {object_list} from the {bucket_name} bucket")
            errors = self.get_minio_client().remove_objects(bucket_name=bucket_name,
                                                            delete_object_list=object_list)
            return  errors

        except Exception as e:
            minio_except.error(f"Exception occured when removing objects: {object_list}: {e}")


if __name__ == f"__main__":
    s3_obj = MinUtils(config_section=bc.aws_config_section,
                      target_bucket_name=bc.s3_raw_bucket,
                      )
    # print(f" Get the minio client: {s3_obj.get_minio_client()}")
    minio_logger.info(f" Get the minio client: {s3_obj.get_minio_client()}")

    output_data = s3_obj.load_file_paths(
        local_path=bc.DATA_PATH,
        bucket_name=bc.s3_raw_bucket,
        minio_path=f'inputs')
    minio_logger.info(f"\nFile status: {output_data}")
