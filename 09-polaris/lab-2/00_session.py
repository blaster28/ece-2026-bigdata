# SparkSession with two Iceberg catalogs: "lab" (Hadoop, lab 07) and "polaris" (REST)
# Run inside the pyspark shell: exec(open("00_session.py").read())
import os
import configparser

PRINCIPAL = os.environ.get("POLARIS_PRINCIPAL", "dev")  # "dev" or "analyst"
CREDENTIAL = os.environ[f"POLARIS_{PRINCIPAL.upper()}_CREDENTIAL"]
BUCKET = os.environ["KUBERNETES_NAMESPACE"]
POLARIS_URI = os.environ.get("POLARIS_URI", "http://polaris:8181")
POLARIS_CATALOG = os.environ.get("POLARIS_CATALOG", BUCKET)

# S3A with the profile credentials, not the expired AWS_* variables of the pod
cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser("~/.aws/credentials"))
creds = cfg["default"]
hconf = spark.sparkContext._jsc.hadoopConfiguration()
for key, value in {
    "fs.s3a.endpoint": "https://s3.seaweedfs.adm.adaltas.cloud",
    "fs.s3a.endpoint.region": "us-east-1",
    "fs.s3a.path.style.access": "true",
    "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    "fs.s3a.access.key": creds["aws_access_key_id"],
    "fs.s3a.secret.key": creds["aws_secret_access_key"],
    "fs.s3a.session.token": creds["aws_session_token"],
    "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    # Polaris returns s3:// locations: route them to the S3A connector
    "fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}.items():
    hconf.set(key, value)
spark._jvm.org.apache.hadoop.fs.FileSystem.closeAll()

# Catalogs are runtime SQL settings, read the first time each catalog is used
for key, value in {
    "spark.sql.catalog.lab": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lab.type": "hadoop",
    "spark.sql.catalog.lab.warehouse": f"s3a://{BUCKET}/warehouse",
    "spark.sql.catalog.polaris": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.polaris.type": "rest",
    "spark.sql.catalog.polaris.uri": f"{POLARIS_URI}/api/catalog",
    "spark.sql.catalog.polaris.warehouse": POLARIS_CATALOG,
    "spark.sql.catalog.polaris.credential": CREDENTIAL,
    "spark.sql.catalog.polaris.scope": "PRINCIPAL_ROLE:ALL",
    # No credential vending on this storage: data files are read and written with the S3A keys above
    "spark.sql.catalog.polaris.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
}.items():
    spark.conf.set(key, value)

spark.sparkContext.setLogLevel("ERROR")
print("Polaris principal:", PRINCIPAL, "| S3 key: ..." + creds["aws_access_key_id"][-4:],
      "| fs.s3.impl:", hconf.get("fs.s3.impl"),
      "| extensions:", spark.conf.get("spark.sql.extensions", "none"))
