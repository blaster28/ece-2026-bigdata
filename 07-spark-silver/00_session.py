import os
import configparser
from pyspark.sql import SparkSession

BUCKET = os.environ["KUBERNETES_NAMESPACE"]
cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser("~/.aws/credentials"))
creds = cfg["default"]

spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .config("spark.hadoop.fs.s3a.endpoint", "https://s3.seaweedfs.adm.adaltas.cloud")
    .config("spark.hadoop.fs.s3a.access.key", creds["aws_access_key_id"])
    .config("spark.hadoop.fs.s3a.secret.key", creds["aws_secret_access_key"])
    .config("spark.hadoop.fs.s3a.session.token", creds["aws_session_token"])
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("key:", creds["aws_access_key_id"][-4:], "| master:", spark.sparkContext.master,
      "| extensions:", spark.conf.get("spark.sql.extensions", "none"))
print(spark.read.text(f"s3a://{BUCKET}/bronze/users.csv").count(), "lines read from S3")
