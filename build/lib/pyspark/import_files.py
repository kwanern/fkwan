from pyspark.sql.types import *
from pyspark.sql import functions as sqlf
from pyspark.sql import Window


def import_list_old(date_range, base_path, header, delimiter):
    fields = [(StructField(field, StringType(), True)) for field in header]
    svc_schema = StructType(fields)
    paths = list(map(lambda x: base_path.format(x), date_range))
    df = spark.read.option("header", "true").option("delimiter", delimiter).csv(paths, schema=svc_schema)
    return df


def import_list(date_range, base_path, delimiter, header_names=None):
    df = None
    paths = list(map(lambda x: base_path.format(x), date_range))

    if header_names == None:
        header = "true"

        temp = spark.read.option("delimiter", delimiter) \
            .option("header", header) \
            .option("inferSchema", "True") \
            .csv(paths[0])

        df = spark.read \
            .option("header", header) \
            .option("delimiter", delimiter) \
            .csv(paths, schema=temp.schema)

    else:
        header = "false"

        temp = spark.read.option("delimiter", delimiter) \
            .option("header", header) \
            .option("inferSchema", "True") \
            .csv(paths[0]) \
            .toDF(*header_names)

        df = spark.read \
            .option("header", header) \
            .option("delimiter", delimiter) \
            .csv(paths, schema=temp.schema) \
            .toDF(*header_names)
    return df


def read_table_azure_latest(file_path, text_name):
    header = dbutils.fs.head(file_path + "_HEADER.txt").strip().split("\x01")
    fields = [(StructField(field, StringType(), True)) for field in header]
    svc_schema = StructType(fields)
    return spark.read.option("delimiter", "\x01").csv(file_path + text_name, schema=svc_schema)


def spark_read_csv(file_path, delimiter=",", header_names=None):
    df = None
    if header_names is None:
        header = "true"
        df = spark.read.format("csv") \
            .option("header", header) \
            .option("delimiter", delimiter) \
            .load(file_path) \
            .toDF()
    else:
        header = "false"
        df = spark.read.format("csv") \
            .option("header", header) \
            .option("delimiter", delimiter) \
            .load(file_path) \
            .toDF(*header_names)
    return df