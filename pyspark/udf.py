from ..libraries import *
from pyspark.sql.functions import pandas_udf, PandasUDFType, udf
from functools import reduce
from pyspark.sql import DataFrame


def union_all(*dfs):
    """
        This function union multiple spark dataframe

        :param dfs: array of spark dataframe
        :return: spark dataframe

        Examples:
        >>> union_all(*[a.pf_spdf for a in customers])
    """
    return reduce(DataFrame.union, dfs)


@udf("string")
def concat_string_arrays(*ls):
    """
        This function concat multiple string columns into one column with separator '&'

        :param ls: array of column names
        :return: string column

        Examples:
        >>> concat_string_arrays(*[re.sub("\s", "_", i) for i in product_names])
    """
    return ' & '.join(filter(None, ls))
