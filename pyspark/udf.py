from ..libraries import *


@sqlf.udf("string")
def concat_string_arrays(*ls):
    """
        This function concat multiple string columns into one column with separator '&'

        :param ls: array of column names
        :return: string column

        Examples:
        >>> concat_string_arrays(*[re.sub("\s", "_", i) for i in products_names])
    """
    return ' & '.join(filter(None, ls))
