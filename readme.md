# Apache Hudi with MinIO and Spark

This repository demonstrates how to set up and use **Apache Hudi** with **MinIO** (as local object storage) and **Apache Spark** for building a transactional data lakehouse. It includes configurations, prerequisites, and code examples to get started with Hudi tables using Spark as the compute engine.

---

## Introduction to Apache Hudi

**Apache Hudi** (pronounced "Hoodie") stands for **Hadoop Upserts, Deletes, and Incrementals**. It is an open-source transactional data lakehouse platform built around a database kernel. Hudi provides table-level abstractions over open file formats like **Apache Parquet** and **ORC**, delivering core warehouse and database functionalities directly in the data lake. It also supports **ACID transactions**, **upserts**, **deletes**, and **incremental processing**.

Currently, **Apache Spark** is the simplest solution for creating and maintaining Hudi tables. While other engines like **DuckDB** support formats like **Iceberg** and **Delta Lake**, Hudi support is still evolving. This project uses Spark's built-in catalog for handling metadata, but future updates may explore alternatives like **Apache Hive**.

---

## Prerequisites

To run this project, you’ll need the following:

1. **MinIO**: For local object storage (S3-compatible).
2. **Apache Spark (3.5.4)**: For compute and cataloging.
3. **Apache Hudi (1.0.0)**: For building the data lakehouse.
4. **Spark Jars**:
   - `org.apache.hadoop:hadoop-aws:3.3.4`
   - `com.amazonaws:aws-java-sdk-bundle:1.12.262`
   - `org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.0`

---

## Setup Instructions

### 1. Install Dependencies
- Ensure you have **Python 3.x** installed.
- Install **PySpark** and required dependencies using `pip`:
  ```bash
  pip install pyspark==3.5.4

### 2. Configure Hadoop and MinIO
- Ensure you get the correct version of Hadoop from your Spark environment
- You can usually search within this folder in Windows, 
```venv\Lib\site-packages\hadoop```
- MinIO is usually started with:
  ```bash
  minio server /path/to/data ```

### 3. Configure Spark and MinIO
- Add the following configurations to your Spark session to connect to MinIO:
  ```bash
  from pyspark.sql import SparkSession
    
  spark = SparkSession.builder \
    .appName("Hudi with MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_MINIO_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_MINIO_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()```

### 4. Running the project
- Clone the repo
  ```bash
  git clone https://github.com/your-username/your-repo-name.git
  cd your-repo-name
  
- Update the MinIO access and secret keys in spark config
- Run the spark app by,
  ```bash
  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.0 hudi_spark.py

## Future Enhancements
* Explore integration with Apache Hive or other metadata catalogs.
* Add support for additional query engines like DuckDB or Trino.
* Implement advanced Hudi features like time travel and compaction.

## Resources
* [Apache Hudi Documentation](https://hudi.apache.org/docs/next/overview/)
* [MinIO Documentation](https://min.io/docs/minio/windows/index.html)
* [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
* Inspired from this [blog](https://blog.min.io/streaming-data-lakes-hudi-minio/)

## License
* This project is licensed under the Apache License 2.0. See the LICENSE file for details.

## Acknowledgement
* Credits to [Koedit](https://stackoverflow.com/a/77339438) for the AWS SDK compatibility reference.
* Apache Hudi, MinIO, and Apache Spark communities for their excellent documentation and support.
