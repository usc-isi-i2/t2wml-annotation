import os
import pandas as pd
from requests import get, post, put, delete


class Utility(object):
    def __init__(self):
        pass

    @staticmethod
    def find_data_start_row(df: pd.DataFrame) -> (int, int):
        # finds and returns header and data row index
        header_row = 6
        data_row = 7

        if "header" in df.index:
            header_row = df.index.tolist().index("header")
            if "data" not in df.index:
                data_row = header_row + 1

        if "data" in df.index:
            data_row = df.index.tolist().index("data")

        return header_row, data_row

    @staticmethod
    def upload_data(file_path, url, method=post):
        file_name = os.path.basename(file_path)
        files = {
            'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream')
        }
        response = method(url, files=files)
        return response
