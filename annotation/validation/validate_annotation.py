import pandas as pd
import json


class VaidateAnnotation(object):
    def __init__(self):
        self.error_report = []

    def validate(self, file_path=None, df=None):
        if file_path is None and df is None:
            raise Exception('Please provide a file path or a dataframe to validate')
        if file_path:
            df = pd.read_excel(file_path).fillna('')

        valid_column_one = self.validate_annotation_column_one(df)
        if not valid_column_one:
            raise Exception(json.dumps(self.error_report))

        

    def validate_annotation_column_one(self, df):
        valid_first_column = True
        if df.iloc[0, 0].strip().lower() != 'dataset':
            valid_first_column = False
            self.error_report.append(
                self.error_row('Incorrect annotation: First Column', 1, 1, 'First row in column 1 should be "dataset"'))

        if df.iloc[1, 0].strip().lower() != 'role':
            valid_first_column = False
            self.error_report.append(
                self.error_row('Incorrect annotation: First Column', 2, 1, 'Second row in column 1 should be "role"'))

        if df.iloc[0, 0].strip().lower() != 'type':
            valid_first_column = False
            self.error_report.append(
                self.error_row('Incorrect annotation: First Column', 3, 1, 'Third row in column 1 should be "type"'))

        if df.iloc[0, 0].strip().lower() != 'description':
            valid_first_column = False
            self.error_report.append(
                self.error_row('Incorrect annotation: First Column', 4, 1,
                               'Fourth row in column 1 should be "description"'))

        if df.iloc[0, 0].strip().lower() != 'name':
            valid_first_column = False
            self.error_report.append(
                self.error_row('Incorrect annotation: First Column', 5, 1, 'Fifth row in column 1 should be "name"'))

        if df.iloc[0, 0].strip().lower() != 'unit':
            valid_first_column = False
            self.error_report.append(
                self.error_row('Incorrect annotation: First Column', 6, 1, 'Sixth row in column 1 should be "unit"'))

        return valid_first_column

    @staticmethod
    def error_row(error, row, column, description):
        return {
            'Error': error,
            'Line Number': row,
            'Column': column,
            'Description': description
        }


va = VaidateAnnotation()
df = pd.read_excel('sample_annotation.xlsx', header=None)
va.validate_annotation_column_one(df)
