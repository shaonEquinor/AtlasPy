from datetime import datetime, timedelta
import pytest
from atlas import functions


class Test_date_range:

    @pytest.mark.parametrize("start_date, end_date, step, length", [
        (datetime(2010, 1, 1), datetime(2011, 1, 1), timedelta(days=30), 13),
        (datetime(2010, 1, 1), datetime(2010, 1, 20), timedelta(days=5), 4),
        (datetime(2010, 1, 1), datetime(2010, 1, 20), timedelta(days=365), 1),
    ])
    def test_length(self, start_date, end_date, step, length):
        split_dates = [(start, end) for start, end in functions.date_range(start_date, end_date, step)]
        assert len(split_dates) == length

    # def test_proper_split(self, ):

    # assert dates[3] == (datetime.datetime(2020, 3, 31, 0, 0), datetime.datetime(2020, 4, 1, 0, 0))
