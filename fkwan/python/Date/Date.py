from ...libraries import *


class Date(object):
    def __init__(self, dt):
        self.date = pd.to_datetime(pd.Timestamp(dt)).date()

    def __repr__(self):
        return str(self.date)

    # adding days to date
    def __add__(self, x):
        if type(x) is int:
            return self.date + pd.Timedelta(x, unit='d')
        else:
            return "Wrong Operator"

    # subtracting days from date
    def __sub__(self, x):
        if type(x) is int:
            return self.date + pd.Timedelta(-x, unit='d')
        else:
            return self.date - x.date


def date_range(start_date, end_date, format='%Y%m%d', freq='D'):
    """
        This function creates an array of date integer, by 1 day

        :param start_date: string
        :param end_date: string
        :param format: string
        :param freq: string
        :return: array of date

        Examples:
        >>> date_range('20190101', '20190201', format='%Y%m%d', freq='2D')
    """
    return [str(d.strftime(format)) for d in pd.date_range(str(start_date.date), str(end_date.date), freq=freq)]

