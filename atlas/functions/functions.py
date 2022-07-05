from datetime import datetime, timedelta
from contextlib import contextmanager
import paramiko
import os
from os import path
import time


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


@contextmanager
def sftp_context(host: str, username: str, password: str, port: int = 22) -> paramiko.sftp_client.SFTPClient:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=username, password=password, port=port)
    sftp = ssh.open_sftp()
    try:
        yield sftp
    finally:
        sftp.close()
        ssh.close()


class Timestamper:
    __load_status = False

    def __init__(self, last_loaded, this_loaded):
        self.last_load = last_loaded
        self.this_load = this_loaded

    def get_status(self):
        return self.__load_status

    def update(self):
        self.__load_status = True


@contextmanager
def timestamper_context(timestamp_file: str, default='0'):
    last_load = default
    this_load = int(time.time())

    timestamp_dir = os.path.dirname(os.path.realpath(timestamp_file))

    if path.isfile(timestamp_file):
        with open(timestamp_file, "r") as file:
            last_load = file.readline()
    elif path.isdir(timestamp_dir) is False:
        os.makedirs(timestamp_dir)

    timestamper = Timestamper(last_load, this_load)

    try:
        yield timestamper
    finally:
        if timestamper.get_status():
            with open(timestamp_file, 'w') as file_writer:
                file_writer.write(str(timestamper.this_load))
                print(f"Updated timestamp file,\tOld: {timestamper.last_load}\tNew: {timestamper.this_load}")
        else:
            print(f"update() not called. {timestamp_file} file not updated")
