import pandas as pd


def days(day):
    return pd.Timedelta(day, unit='d')


# Read Table Function
def date_range(start_date, end_date):
    return [int(d.strftime('%Y%m%d')) for d in pd.date_range(start_date, end_date)]
