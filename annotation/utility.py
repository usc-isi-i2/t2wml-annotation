import os
import pandas as pd
from requests import post
from enum import Enum


class Category(Enum):
    DATASET = 'dataset'
    ROLE = 'role'
    TYPE = 'type'
    DESCRIPTION = 'description'
    NAME = 'name'
    UNIT = 'unit'
    HEADER = 'header'
    DATA = 'data'


class Utility(object):
    def __init__(self):
        pass

    @staticmethod
    def find_data_start_row(df: pd.DataFrame) -> (int, int):
        header_index = Utility.get_index(df.iloc[:, 0], Category.HEADER.value)
        data_index = Utility.get_index(df.iloc[:, 0], Category.DATA.value)

        return header_index, data_index

    @staticmethod
    def upload_data(file_path, url, method=post):
        file_name = os.path.basename(file_path)
        files = {
            'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream')
        }
        response = method(url, files=files)
        return response

    @staticmethod
    def get_index(series: pd.Series, value, *, pos=0) -> int:
        return int(series[series == value].index[pos])
