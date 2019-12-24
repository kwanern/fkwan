def days(day):
    """
        This function adds/subtracts day from a datetime object

        :param day: integer
        :return: timedelta object

        Examples:
        >>> days(10)
    """
    return pd.Timedelta(day, unit='d')


# Read Table Function
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
    return [int(d.strftime(format)) for d in pd.date_range(start_date, end_date, freq=freq)]


# Convert time delta to days_hours_minutes
def convert_td(td):
    """
        This function returns timedelta attributes

        :param ts: timedelta
        :return: array of integer

        Examples:
        >>> convert_td(dateime.now() - dateime.now())
    """
    return td.days, td.seconds // 3600, (td.seconds // 60) % 60
