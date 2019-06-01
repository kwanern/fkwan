from pyspark.sql import functions as sqlf


@sqlf.udf("string")
def concat_string_arrays(*ls):
    """
        This function concat multiple string columns into one column with separator &
        :param ls: array of column names
        :return: string column
    """
    return ' & '.join(filter(None, ls))
