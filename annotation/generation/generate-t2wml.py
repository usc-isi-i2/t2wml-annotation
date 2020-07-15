import collections
import string
import time
import datetime

from enum import Enum
from pprint import pprint

import pandas as pd

from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

class Category(Enum):
    DATASET = 'dataset'
    ROLE = 'role'
    TYPE = 'type'
    DESCRIPTION = 'description'
    NAME = 'name'
    UNIT = 'unit'
    HEADER = 'header'
    DATA = 'data'

class Role(Enum):
    MAIN_SUBJECT = "main subject"
    TIME = "time"
    LOCATION = "location"
    VARIABLE = "variable"
    QUALIFIER = "qualifier"
    UNIT = "unit"

class Type(Enum):
    STRING = "string"
    NUMBER = "number"
    ADMIN1 = "admin1"
    ADMIN2 = "admin2"
    ADMIN3 = "admin3"
    COUNTRY = "country"
    CITY = "city"
    # ISO_DATE = "ISO date"
    ISO_DATE_TIME = "ISO date time"
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    POINT = "point"
    ENTITY = "entity"

property_node = {
    Type.COUNTRY: 'P17',
    Type.ADMIN1: 'P2006190001',
    Type.ADMIN2: 'P2006190002',
    Type.ADMIN3: 'P2006190003',
    Type.ISO_DATE_TIME: 'P585',
    Type.POINT: 'P276'
}

date_format = {
    Type.YEAR: '%Y',
    Type.MONTH: '%m',
    Type.DAY: '%d',
    # Type.ISO_DATE: '%Y-%m-%d',
    Type.ISO_DATE_TIME: '%Y-%m-%dT%H:%M:%S%Z'
}

def to_letter_column(number):
    assert number >= 0
    length = 1
    size = 26
    while number >= size:
        length += 1
        number -= size
        size *= 26
    result = ['A']*length
    index = length - 1
    while number > 0:
        rem = number % 26
        number = number // 26
        result[index] = string.ascii_uppercase[rem]
        index -= 1
    return ''.join(result)

def to_number_column(col: string):
    result = 0
    size = 26
    base = 0
    for i, letter in enumerate(col):
        index = string.ascii_uppercase.index(letter)
        result = 26 * result + index
        if i > 0:
            base = base + size
            size *= 26
    result = result + base
    return result

def get_index(series: pd.Series, value, *, pos=0) -> int:
    return int(series[series == value].index[pos])

def get_indices(series: pd.Series, value, *, within: pd.Int64Index = None) -> pd.Int64Index:
    if within is not None:
        return within.intersection(series[series == value].index)
    else:
        return series[series == value].index

class ToT2WML:
    def __init__(self, annotated_spreadsheet: pd.DataFrame):
        self.sheet = annotated_spreadsheet

        # category rows
        self.role_index = get_index(self.sheet.loc[:, 0], Category.ROLE.value)
        self.type_index = get_index(self.sheet.loc[:, 0], Category.TYPE.value)
        self.header_index = get_index(self.sheet.loc[:, 0], Category.HEADER.value)
        self.data_index = get_index(self.sheet.loc[:, 0], Category.DATA.value)

        # role
        self.main_subject_index = get_index(self.sheet.loc[self.role_index, :], Role.MAIN_SUBJECT.value)
        self.time_indcies = get_indices(self.sheet.loc[self.role_index, :], Role.TIME.value)
        self.location_indices = get_indices(self.sheet.loc[self.role_index, :], Role.LOCATION.value)
        self.variable_indices = get_indices(self.sheet.loc[self.role_index, :], Role.VARIABLE.value)
        self.qualifier_indices = get_indices(self.sheet.loc[self.role_index, :], Role.QUALIFIER.value)
        self.variable_columns = self.sheet.loc[1, :] == Role.VARIABLE.value

    def _get_region(self) -> dict:
        top = self.data_index
        bottom = self.sheet.shape[0]
        left = get_index(self.sheet.loc[1, :], Role.VARIABLE.value)
        right = get_index(self.sheet.loc[1, :], Role.VARIABLE.value, pos=-1)
        region = {
            'left': to_letter_column(left),
            'right': to_letter_column(right),
            'top': top+1,
            'bottom': bottom
        }
        return region

    def _get_time(self) -> dict:
        # add point in time
        # Need to generalize
        if self.time_indcies.shape[0] == 0:
            raise RuntimeError('No column labeled with "time" role')
        iso_indices = get_indices(self.sheet.loc[self.type_index, :], Type.ISO_DATE_TIME,
            within=self.time_indcies)
        if iso_indices.shape[0] > 0:
            time_index = iso_indices.shape[0]
            result = {
                'property': 'P585',
                'value': f'=value[{to_letter_column(time_index)}, $row]',  # '=conat(value[C:E, $row], "-")',
                'calendar': 'Q1985727',
                'precision': 'day',
                'time_zone': 0,
                'format': '%Y-%m-%dT%H:%M:%S%Z'
            }
            return result
        # over multiple columns
        time_cells = []
        time_formats = []
        precision = 'year'
        for col_type in [Type.YEAR, Type.MONTH, Type.DAY]:
            col_indices = get_indices(self.sheet.loc[self.type_index, :],
                col_type.value, within=self.time_indcies)
            if col_indices.shape[0]:
                col_index = get_index(self.sheet.loc[self.type_index, :], col_type.value)
                time_cells.append(col_index)
                time_formats.append(date_format[col_type])
                if col_type == Type.MONTH:
                    precision = 'month'
                elif col_type == Type.MONTH:
                    precision = 'day'
        cells = ', '.join([f'value[{to_letter_column(col)}, $row]' for col in time_cells])
        value = f'=concat({cells}, "-")'
        time_format = '-'.join(time_formats)
        result = {
            'property': 'P585',
            'value': value,  # '=conat(value[C:E, $row], "-")',
            'calendar': 'Q1985727',
            'precision': precision,
            'time_zone': 0,
            'format': time_format
        }
        return result

    def _get_admins(self) -> list:
        # country and admins
        qualifier = []
        for col_type in [Type.COUNTRY, Type.ADMIN1, Type.ADMIN2, Type.ADMIN3, Type.ADMIN3]:
            if get_indices(self.sheet.loc[self.type_index, :], col_type.value).shape[0]:
                col_index = get_index(self.sheet.loc[self.type_index, :], col_type.value)
                entry = {
                    'property': f'{property_node[col_type]}',
                    'value': f'=value[{to_letter_column(col_index)}, $row]',
                }
                qualifier.append(entry)
        return qualifier

    def _get_coordinate(self) -> dict:
        # add coordinate
        # need to generalize
        longitude_index = get_indices(self.sheet.loc[self.type_index, :], Type.LONGITUDE.value)
        latitude_index = get_indices(self.sheet.loc[self.type_index, :], Type.LATITUDE.value)
        if longitude_index.shape[0] and latitude_index.shape[0]:
            longitude_index = longitude_index[0]
            latitude_index = latitude_index[0]
            result = {
                'property': 'P276',
                'value': '=concat("POINT(", value[' + to_letter_column(longitude_index) + ' , $row], value[' \
                    + to_letter_column(latitude_index) + ', $row], ")", " ")'
            }
            return result

    def _get_qualifiers(self) -> list:
        # add qualifiers
        qualifier = []
        if self.qualifier_indices.shape[0] == 0:
            return qualifier
        for i in range(self.qualifier_indices.shape[0]):
            col_index = self.qualifier_indices[i]
            entry = {
                'property': f'=item[{to_letter_column(col_index)}, {self.header_index + 1}, "property"]',
                'value': f'=value[{to_letter_column(col_index)}, $row]'
            }
            qualifier.append(entry)
        return qualifier

    def get_dict(self) -> dict:
        region = self._get_region()

        # template = collections.OrderedDict()
        template = dict()
        template['item'] = f'=item[{to_letter_column(self.main_subject_index)}, $row, "main subject"]'
        template['property'] = f'=item[$col, {self.header_index+1}, "property"]'
        template['value'] = '=value[$col, $row]'

        qualifier = []
        qualifier.append(self._get_time())
        qualifier += self._get_admins()
        qualifier.append(self._get_coordinate())
        qualifier += self._get_qualifiers()
        template['qualifier'] = qualifier

        t2wml_yaml = {
            'statementMapping': {
                'region': [region],
                'template': template,
            }
        }

        return t2wml_yaml

    def get_yaml(self) -> str:
        t2wml_yaml = self.get_dict()
        output = dump(t2wml_yaml, Dumper=Dumper)
        return output

if __name__ == '__main__':
    input_file = 'aid worker security_incidents2020-06-22.xlsx'
    sheet = pd.read_excel(input_file, header=None)
    sheet.loc[1, :] = sheet.loc[1, :].fillna(method='ffill')
    to_t2wml = ToT2WML(sheet)
    print(to_t2wml.get_yaml())
