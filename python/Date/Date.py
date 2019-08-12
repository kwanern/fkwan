from ...libraries import *


class Date(object):
    def __init__(self, dt):
        self.date = pd.to_datetime(pd.Timestamp(dt))

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

