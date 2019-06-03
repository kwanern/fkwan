from ..libraries import *


def days(day):
    """
        This function adds/subtracts day from a datetime object
        :param day: integer
        :return: timedelta object
    """
    return pd.Timedelta(day, unit='d')


# Read Table Function
def date_range(start_date, end_date):
    """
        This function adds/subtracts day from a datetime object
        :param start_date: string, e.g. '20190101'
        :param end_date: string, e.g. '20190201'
        :return: array of date integer
    """
    return [int(d.strftime('%Y%m%d')) for d in pd.date_range(start_date, end_date)]
