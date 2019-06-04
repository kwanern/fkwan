from ..libraries import *
from pyspark.sql.functions import pandas_udf, PandasUDFType


@pandas_udf("string", PandasUDFType.GROUPED_MAP)
def concat_string_arrays(*ls):
    """
        This function concat multiple string columns into one column with separator '&'

        :param ls: array of column names
        :return: string column

        Examples:
        >>> concat_string_arrays(*[re.sub("\s", "_", i) for i in product_names])
    """
    return ' & '.join(filter(None, ls))
