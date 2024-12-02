# import os
import config.base_config as bc
from utils.minio_utils import MinUtils
from utils.duckwrapper import DuckDB
from utils.common_actions import CommonActions
from utils.codelogger import basiclogger

main_logger = basiclogger(logname=f'MAIN_LOG', section=f'DEFAULT')
main_except = basiclogger(logname=f'MAIN_EXCEPTION', section=f'EXCEPTION', format_type=f'extended')

if __name__ == f"__main__":
    min_obj = MinUtils(config_section=bc.aws_config_section)
    objects = min_obj.list_minio_objects(bucket_name=bc.s3_comp_bucket)
    duck_con = DuckDB(minio_host=min_obj.parsed_host,
                      minio_access_key=min_obj.aws_access_key_id,
                      minio_secret_key=min_obj.aws_secret_access_key)
    duck_con.connect()
    comm_obj = CommonActions()

    try:
        for obj in objects:
            main_logger.info(f"S3 URI: s3://{obj.bucket_name}/{obj.object_name}")
            s3_obj = comm_obj.get_full_object_path(obj.bucket_name, obj.object_name)
            tbl_name = comm_obj.get_base_file_name(obj.object_name, 'N')
            main_logger.info(f"Table in duck: {tbl_name}")
            duck_con.create_table_from_minio(table_name=tbl_name, s3_path=s3_obj)
            # appending the base filename to the target key
            # query = f"SELECT * FROM '{s3_obj}' LIMIT 1;"
            # print(duck_con.execute_sql(_query_=query))

    except Exception as e:
        main_except.error(f'Exception occurred when creating table: {tbl_name}')
        raise f'MAIN TABLE CREATE exception: {e}'

    query_to_execute = f""" SELECT rac.name||' '||rac.year as race_name, dr.forename||' '||dr.surname AS driver_name,  
        FROM results rs 
        JOIN races rac USING (raceId)
        JOIN drivers dr USING (driverId)
        LIMIT 10; """

    try:
        main_logger.info(f'Query to execute: {query_to_execute}')
        print(duck_con.execute_sql(query_to_execute))
        duck_con.export_db_to_disk()
        duck_con.close_connection()

    except Exception as ex:
        main_except.error(f'Error when executing query: {query_to_execute} \n{ex}')
        raise f'MAIN QUERY execution exception: {ex}'
