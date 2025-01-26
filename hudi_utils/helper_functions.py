import json
import os
from config import base_config as bc


def load_config(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)


# Load Spark configuration
def get_spark_config():
    return load_config(bc.SPARK_CONF)


# Load Hudi table-specific configuration
def get_hudi_config(table_name):
    file_path = os.path.join("config", "hudi_config", f"{table_name}_config.json")
    return load_config(file_path)
