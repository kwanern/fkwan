from ..libraries import *
from ..python import *


def import_list_old(date_range, base_path, header, delimiter):
    """
        This function imports a list of raw file (old and archived).

        :param date_range: array
        :param base_path: string
        :param header: array
        :param delimiter: character
        :return: spark dataframe

        Examples:
        >>> import_list_old(
        >>>     date_range('20190101', '20190201'),
        >>>     "/mnt/workspaces/customeranalytics/dev/dmb/feature_mart/day/",
        >>>     hdr,
        >>>     ","
        >>> )
    """
    fields = [(StructField(field, StringType(), True)) for field in header]
    svc_schema = StructType(fields)
    paths = list(map(lambda x: base_path.format(x), date_range))
    df = spark.read.option("header", "true").option("delimiter", delimiter).csv(paths, schema=svc_schema)
    return df


def import_list(date_range, base_path, delimiter, header_names=None):
    """
        This function imports a list of raw file.

        :param date_range: array
        :param base_path: string
        :param delimiter: character
        :param header_names: boolean
        :return: spark dataframe

        Examples:
        >>> import_list(
        >>>     date_range('20190101', '20190201'),
        >>>     "/mnt/workspaces/customeranalytics/dev/dmb/feature_mart/day/",
        >>>     ",",
        >>>     True
        >>> )
    """
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
    """
        This function imports csv file into spark dataframe.

        :param file_path: string
        :param delimiter: character
        :param header_names: boolean
        :return: spark dataframe

        Examples:
        >>> spark_read_csv(
        >>>     "/mnt/workspaces/customeranalytics/dev/dmb/feature_mart/day/",
        >>>     ",",
        >>>     True
        >>> )
    """
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