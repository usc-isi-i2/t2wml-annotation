import pandas as pd
import json
from xlsxwriter import utility

ROLE_ROW = 2
TYPE_ROW = 3


class VaidateAnnotation(object):
    def __init__(self):
        self.error_report = []
        self.valid_roles = {
            'main subject': 1,
            'time': 1,
            'location': 1,
            'variable': 1,
            'qualifier': 1,
            'unit': 1
        }
        self.valid_types = {
            'main subject': ['string', 'entity'],
            'time': ['year', 'month', 'day'],
            'location': ['admin1', 'admin2', 'admin3', 'longitude', 'latitude', 'country', 'city'],
            'variable': ['number'],
            'qualifier': ['string'],
            'unit': ['string']
        }

    def validate(self, file_path=None, df=None):
        if file_path is None and df is None:
            raise Exception('Please specify a file path or a pandas DataFrame')
        if file_path is not None:
            df = pd.read_excel(file_path, header=None).fillna('')

        valid_column_one = self.validate_annotation_column_one(df)
        if not valid_column_one:
            raise Exception(json.dumps(self.error_report))

        valid_roles = self.validate_roles_types(df)
        if not valid_roles:
            raise Exception(json.dumps(self.error_report))

        valid_role_and_type = self.validate_roles_types(df)
        if not valid_role_and_type:
            raise Exception(json.dumps(self.error_report))

    def validate_roles_types(self, df):
        roles = list(df.iloc[1])
        types = list(df.iloc[2])
        invalid_roles = []

        for i in range(1, len(roles)):
            if roles[i].strip().split(';')[0] not in self.valid_roles and roles[i] != '':
                invalid_roles.append(utility.xl_col_to_name(i))

        if invalid_roles:
            self.error_report.append(
                self.error_row('Roles in following column(s) are invalid: {}'.format(','.join(invalid_roles)),
                               ROLE_ROW, ','.join(invalid_roles),
                               'Valid roles are one of the following: {}'.format(','.join(self.valid_roles))))
            return False

        valid_types = True
        for i in range(1, len(types)):
            r = roles[i].strip().split(';')[0]
            t = types[i].strip()

            if t == '' and r != '':
                self.error_report.append(self.error_row(
                    'Missing TYPE for role: {}'.format(r),
                    TYPE_ROW,
                    utility.xl_col_to_name(i),
                    'Please specify a valid type. Valid type for role: {} is one of the following: [{}]'.
                        format(r, ','.join(self.valid_types[r]))
                ))
                valid_types = False
            elif t != '' and r == '':
                self.error_report.append(self.error_row(
                    'Missing ROLE for type: {}'.format(t),
                    ROLE_ROW,
                    utility.xl_col_to_name(i),
                    'Specifying TYPE without a ROLE is invalid'
                ))
                valid_types = False

            elif t != '':
                if r == 'time':
                    if t not in self.valid_types[r] and not t.startswith('%'):
                        self.error_report.append(self.error_row(
                            'Invalid type: {}, for the role: {}'.format(t, r),
                            TYPE_ROW,
                            utility.xl_col_to_name(i),
                            'Valid type for role: {}, is either one of [{}], OR a python date format '
                            'regex(https://docs.python.org/3.7/library/datetime.html#strftime-and-strptime-behavior)'
                                .format(r, ','.join(self.valid_types[r]))
                        ))
                        valid_types = False
                else:
                    if t not in self.valid_types[r]:
                        self.error_report.append(self.error_row(
                            'Invalid type: {}, for the role: {}'.format(t, r),
                            TYPE_ROW,
                            utility.xl_col_to_name(i),
                            'Valid type for role: {}, is [{}]'.format(r, ','.join(self.valid_types[r]))
                        ))
                        valid_types = False

        return valid_types

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

        if df.iloc[2, 0].strip().lower() != 'type':
            valid_first_column = False
            self.error_report.append(
                self.error_row('Incorrect annotation: First Column', 3, 1, 'Third row in column 1 should be "type"'))

        if df.iloc[3, 0].strip().lower() != 'description':
            valid_first_column = False
            self.error_report.append(
                self.error_row('Incorrect annotation: First Column', 4, 1,
                               'Fourth row in column 1 should be "description"'))

        if df.iloc[4, 0].strip().lower() != 'name':
            valid_first_column = False
            self.error_report.append(
                self.error_row('Incorrect annotation: First Column', 5, 1, 'Fifth row in column 1 should be "name"'))

        if df.iloc[5, 0].strip().lower() != 'unit':
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
