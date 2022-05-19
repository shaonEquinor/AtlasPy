from datetime import datetime, timedelta


def date_range(start_date: datetime, end_date: datetime, step: timedelta):
    """
    Takes start, end date and step size as input and returns a pair of dates from start date to end date each with
    the length of step. Can be helpful when ingesting data over a large period, so you have to divide the date range in
    smaller date ranges
    :param start_date: Start of the range
    :param end_date: End of the range
    :param step: Size of the smaller range
    :return: pairs of smaller date range in the length of step size
    """
    assert end_date > start_date, f'start_date should not be bigger than end_date. \nstart_date:{start_date} \t ' \
                                  f'end_date:{end_date} '
    assert step.total_seconds() > 0.0, f'delta_date should be positive. \ndelta_date: {step.total_seconds()} total ' \
                                       f'seconds '
    cur_date = start_date
    while cur_date + step < end_date:
        yield cur_date, cur_date + step
        cur_date = cur_date + step
    yield cur_date, end_date
