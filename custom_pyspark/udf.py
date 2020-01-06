#from ..libraries import *
from pyspark.sql.functions import pandas_udf, PandasUDFType, udf
from pyspark.sql.types import StringType
from functools import reduce
from pyspark.sql import DataFrame
import numpy as np
import pandas as pd


def union_all(*dfs):
    """
        This function union multiple spark dataframe

        :param dfs: array of spark dataframe
        :return: spark dataframe

        Examples:
        >>> union_all(*[a.pf_spdf for a in customers])
    """
    return reduce(DataFrame.union, dfs)


def concat(*ls):
    """
        This function concat multiple string columns into one column with separator '&'

        :param ls: array of column names
        :return: string column

        Examples:
        >>> concat_string_arrays(*[re.sub("\s", "_", i) for i in product_names])
    """
    return ' & '.join(filter(None, ls))

concat_string_arrays = udf(concat, StringType())


def describe_pd(df_in, columns, deciles=False):
    """
        This function return summary statistics of a spark dataframe

        :param df_in: spark dataframe
        :param columns: array of strings
        :return: pandas

        Examples:
        >>> concat_string_arrays(*[re.sub("\s", "_", i) for i in product_names])
    """
    if deciles:
        percentiles = np.array(range(0, 110, 10))
    else:
        percentiles = [25, 50, 75]

    percs = np.transpose([np.percentile(df_in.select(x).collect(), percentiles) for x in columns])
    percs = pd.DataFrame(percs, columns=columns)
    percs['summary'] = [str(p) + '%' for p in percentiles]

    spark_describe = df_in.describe().toPandas()
    new_df = pd.concat([spark_describe, percs],ignore_index=True)
    new_df = new_df.round(2)
    return new_df[['summary'] + columns]