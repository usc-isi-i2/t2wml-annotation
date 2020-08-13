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
    ISO_DATE_TIME = "iso"
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
    Type.POINT: 'P625',  # 'P276'
}

location_context = {
    Type.COUNTRY: 'country',
    Type.ADMIN1: 'admin1',
    Type.ADMIN2: 'admin2',
    Type.ADMIN3: 'admin3'
}

date_format = {
    Type.YEAR: '%Y',
    Type.MONTH: '%m',
    Type.DAY: '%d',
    # Type.ISO_DATE: '%Y-%m-%d',
    Type.ISO_DATE_TIME: '%Y-%m-%dT%H:%M:%S%Z'
}

MONTH_FULL_NAME = set([
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december"
])

MONTH_ABBREVIATED = set([
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec"
])

def guess_year_format(values: pd.Series):
    values = values[values.notna()]
    two_digit_count = values.apply(lambda x: x < 100).sum()
    if two_digit_count > len(values) - two_digit_count:
        return '%y'
    else:
        return '%Y'

def guess_month_format(values: pd.Series):
    values = values[values.notna()]
    int_count = values.apply(lambda x: isinstance(x, int)).sum()
    str_count = values.apply(lambda x: isinstance(x, str)).sum()
    if int_count >= str_count:
        # numerical month
        return '%m'
    full_name_count = pd.Series(values).apply(lambda x: x in MONTH_FULL_NAME).sum()
    abbreviated_count = pd.Series(values).apply(lambda x: x in MONTH_ABBREVIATED).sum()
    if full_name_count > abbreviated_count:
        return '%B'
    else:
        return '%b'


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

def get_indices(series: pd.Series, value, *, within: pd.Int64Index = None, startswith = False) -> pd.Int64Index:
    if startswith:
        indices = series[series.apply(lambda x: isinstance(x, str) and x.startswith(value))].index
    else:
        indices = series[series == value].index

    if within is not None:
        indices =  within.intersection(indices)

    return indices

class ToT2WML:
    def __init__(self, annotated_spreadsheet: pd.DataFrame, dataset_qnode: str):
        self.sheet = annotated_spreadsheet
        self.dataset_qnode = dataset_qnode

        # category rows
        # self.dataset_index = get_index(self.sheet.iloc[:, 0], Category.DATASET.value)  Not needed
        self.role_index = get_index(self.sheet.iloc[:, 0], Category.ROLE.value)
        self.type_index = get_index(self.sheet.iloc[:, 0], Category.TYPE.value)
        self.unit_index = get_index(self.sheet.iloc[:, 0], Category.UNIT.value)
        self.header_index = get_index(self.sheet.iloc[:, 0], Category.HEADER.value)
        self.data_index = get_index(self.sheet.iloc[:, 0], Category.DATA.value)

        # role
        self.time_indcies = get_indices(self.sheet.iloc[self.role_index, :], Role.TIME.value)
        self.location_indices = get_indices(self.sheet.iloc[self.role_index, :], Role.LOCATION.value)
        self.variable_indices = get_indices(self.sheet.iloc[self.role_index, :], Role.VARIABLE.value)
        self.qualifier_indices = get_indices(self.sheet.iloc[self.role_index, :], Role.QUALIFIER.value)
        self.units_indices = get_indices(self.sheet.iloc[self.role_index, :], Role.UNIT.value, startswith=True)
        self.variable_columns = self.sheet.iloc[1, :] == Role.VARIABLE.value

        self.failures = []

        ## main subject role
        try:
            self.main_subject_index = get_index(self.sheet.iloc[self.role_index, :], Role.MAIN_SUBJECT.value)
        except:
            # use a location column for main subject
            self.main_subject_index = 0
            if self.location_indices.shape[0]:
                for col_type in [Type.ADMIN3, Type.ADMIN2, Type.ADMIN1, Type.COUNTRY]:
                    admin_indices = get_indices(self.sheet.iloc[self.type_index, :], col_type.value)
                    if admin_indices.shape[0]:
                        self.main_subject_index = admin_indices[0]
                        break
            if self.main_subject_index == 0:
                print('WARNING: No columns with "main subject" role annotation, and no "location" roles.')


    def _get_region(self) -> dict:
        try:
            top = self.data_index
            bottom = self.sheet.shape[0]
            left = get_index(self.sheet.iloc[1, :], Role.VARIABLE.value)
            right = get_index(self.sheet.iloc[1, :], Role.VARIABLE.value, pos=-1)
            region = {
                'left': to_letter_column(left),
                'right': to_letter_column(right),
                'top': top+1,
                'bottom': bottom
            }
        except IndexError:
            print('WARNING: No columns with "variable" role annotation.')
            region = {
                'left': 'A  # FIX ME',
                'right': 'A  # FIX ME',
                'top': top+1,
                'bottom': bottom
            }
        return region

    def _get_time_single_format(self) -> dict:
        # over multiple columns
        time_cells = []
        time_formats = []
        precision = 'year'
        try:
            for col_type in [Type.YEAR, Type.MONTH, Type.DAY]:
                col_indices = get_indices(self.sheet.iloc[self.type_index, :],
                                          col_type.value, within=self.time_indcies, startswith=True)
                if col_indices.shape[0]:
                    # col_index = get_index(self.sheet.iloc[self.type_index, :], col_type.value)
                    col_index = col_indices[0]
                    type_spec = self.sheet.iloc[self.type_index, col_index].split(';')
                    if len(type_spec) > 1:
                        spec_format = type_spec[1]
                    else:
                        if col_type == Type.YEAR:
                            spec_format = guess_year_format(self.sheet.iloc[self.data_index:, col_index])
                        elif col_type == Type.MONTH:
                            spec_format = guess_month_format(self.sheet.iloc[self.data_index:, col_index])
                        else:
                            spec_format = date_format[col_type]
                    time_cells.append(col_index)
                    time_formats.append(spec_format)
                    if col_type == Type.MONTH:
                        precision = 'month'
                    elif col_type == Type.DAY:
                        precision = 'day'
        except TypeError:
            print('Failed to guess time format')
        if not time_cells:
            self.failures.append('Failed to guess time format')
            if self.time_indcies.shape[0]:
                value = f'=value[{to_letter_column(self.time_indcies[0])}, $row]  # FIX ME'
            else:
                value = '=value[COL, $row]  # FIX ME'
        elif len(time_cells) > 1:
            cells = ', '.join([f'value[{to_letter_column(col)}, $row]' for col in time_cells])
            value = f'=concat({cells}, "-")'
            time_format = '-'.join(time_formats)
        else:
            value = f'=value[{to_letter_column(time_cells[0])}, $row]'
            time_format = time_formats[0]
        result = {
            'property': 'P585',
            'value': value,  # '=conat(value[C:E, $row], "-")',
            'calendar': 'Q1985727',
            'precision': precision,
            'time_zone': 0,
            'format': time_format
        }
        return result

    def _get_time_multiple_formats(self) -> dict:
        # over multiple columns
        format_list = []
        precision_list = []
        value = ''
        for col_types in [[Type.YEAR, Type.MONTH, Type.DAY], [Type.YEAR, Type.MONTH], [Type.YEAR]]:
            time_cells = []
            time_formats = []
            precision = 'year'
            try:
                for col_type in col_types:
                    col_indices = get_indices(self.sheet.iloc[self.type_index, :],
                                              col_type.value, within=self.time_indcies)
                    if col_indices.shape[0]:
                        # col_index = get_index(self.sheet.iloc[self.type_index, :], col_type.value)
                        col_index = col_indices[0]
                        type_spec = self.sheet.iloc[self.type_index, col_index].split(';')
                        if len(type_spec) > 1:
                            spec_format = type_spec[1]
                        else:
                            if col_type == Type.YEAR:
                                spec_format = guess_year_format(self.sheet.iloc[self.data_index:, col_index])
                            elif col_type == Type.MONTH:
                                spec_format = guess_month_format(self.sheet.iloc[self.data_index:, col_index])
                            else:
                                spec_format = date_format[col_type]
                        time_cells.append(col_index)
                        time_formats.append(date_format[col_type])
                        if col_type == Type.MONTH:
                            precision = 'month'
                        elif col_type == Type.DAY:
                            precision = 'day'
            except TypeError:
                pass
            if not time_cells:
                print('Failed to guess time format')
                time_format = ''
                self.failures.append('Failed to guess time format')
                if self.time_indcies.shape[0]:
                    value = f'=value[{to_letter_column(self.time_indcies[0])}, $row]  # FIX ME'
                else:
                    value = '=value[COL, $row]  # FIX ME'
            elif len(time_cells) > 1:
                cells = ', '.join([f'value[{to_letter_column(col)}, $row]' for col in time_cells])
                if not value:
                    value = f'=concat({cells}, "-")'
                time_format = '-'.join(time_formats)
            else:
                if not value:
                    value = f'=value[{to_letter_column(time_cells[0])}, $row]'
                time_format = time_formats[0]
            if time_format:
                format_list.append(time_format)
            precision_list.append(precision)
        result = {
            'property': 'P585',
            'value': value,  # '=conat(value[C:E, $row], "-")',
            'calendar': 'Q1985727',
            'format': format_list if format_list else '%Y  # FIXE ME',
            'precision': precision_list[0],  # Until precision list is supported, just return first item
            'time_zone': 0,
        }
        return result

    def _get_time(self) -> dict:
        # add point in time
        # Need to generalize
        if self.time_indcies.shape[0] == 0:
            print('WARNING: No columns with "time" role annotation. Using default date 1900-01-01')
            result = {
                'property': 'P585',
                'value': '1900-01-01  # FIX ME',
                'calendar': 'Q1985727',
                'precision': 'day',
                'time_zone': 0,
                'format': '%Y-%m-%d'
            }
            return result

        # Check if type is iso
        iso_indices = get_indices(self.sheet.iloc[self.type_index, :], Type.ISO_DATE_TIME,
                                  within=self.time_indcies)
        if iso_indices.shape[0] > 0:
            time_index = iso_indices.shape[0]
            result = {
                'property': 'P585',
                'value': f'=value[{to_letter_column(time_index)}, $row]',  # '=conat(value[C:E, $row], "-")',
                'calendar': 'Q1985727',
                'precision': 'day',
                'time_zone': 0,
                'format': date_format[Type.ISO_DATE_TIME]
            }
            return result

        # Check if type is a format string
        cell_value = self.sheet.iloc[self.type_index, self.time_indcies[0]]
        if cell_value.startswith('%'):
            result = {
                'property': 'P585',
                'value': f'=value[{to_letter_column(self.time_indcies[0])}, $row]',
                'calendar': 'Q1985727',
                # 'precision': 'day',
                'time_zone': 0,
                'format': cell_value
            }
            return result


        # over multiple columns
        return self._get_time_multiple_formats()
        # return self._get_time_single_format()

    def _get_dataset(self) -> dict:
        # dataset_id = self.sheet.iloc[self.dataset_index, 1]
        result = {
            'property': 'P2006020004',
            'value': self.dataset_qnode
            }
        return result

    def _get_admins(self) -> list:
        # country and admins
        qualifiers = []
        items = []
        for col_type in [Type.ADMIN3, Type.ADMIN2, Type.ADMIN1, Type.COUNTRY]:
            context = location_context[col_type]
            if get_indices(self.sheet.iloc[self.type_index, :], col_type.value).shape[0]:
                col_index = get_index(self.sheet.iloc[self.type_index, :], col_type.value)
                item = f'item[{to_letter_column(col_index)}, $row, "{context}"]'
                value = f'={item}'
                entry = {
                    'property': f'{property_node[col_type]}',
                    'value': value
                }
                qualifiers.append(entry)
                items.append(item)
        if items:
            qualifier = {
                'property': 'P131',
                'value': '=' + ' or '.join(items)
            }
            qualifiers.append(qualifier)
        return qualifiers

    def _get_coordinate(self) -> list:
        # kyao
        # Ignore coordinates for now. 31 July 2020
        return []
        # add coordinate
        longitude_index = get_indices(self.sheet.iloc[self.type_index, :], Type.LONGITUDE.value)
        latitude_index = get_indices(self.sheet.iloc[self.type_index, :], Type.LATITUDE.value)
        if longitude_index.shape[0] and latitude_index.shape[0]:
            longitude_index = longitude_index[0]
            latitude_index = latitude_index[0]
            result = {
                'property': property_node[Type.POINT],
                'latitude': f'=value[{to_letter_column(latitude_index)}, $row]',
                'longitude': f'=value[{to_letter_column(longitude_index)}, $row]',
                'globe': 'wgs84'
                # 'value': '=concat("POINT(", value[' + to_letter_column(longitude_index) + ' , $row], value[' \
                #     + to_letter_column(latitude_index) + ', $row], ")", " ")'
            }
            return [result]
        else:
            return []

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

    def _process_unit_columns(self) -> dict:
        result = dict()
        if self.units_indices.shape[0] == 0:
            return result
        for i in range(self.units_indices.shape[0]):
            col_index = self.units_indices[i]
            unit_spec = self.sheet.iloc[self.role_index, col_index]
            variable_indices = []
            if ';' in unit_spec:
                variable_names = unit_spec.split(';')[1].split(',')
                for name in variable_names:
                    indices = get_indices(self.sheet.iloc[self.header_index,1:], name)
                    if len(indices) == 0:
                        print(f'Invalid unit specification: "{unit_spec}"  No variable named "{name}"')
                    else:
                        variable_indices.append(indices[0])
            if not variable_indices:
                variable_indices = [None]
            for var in variable_indices:
                if var in result:
                    result[var].append(col_index)
                else:
                    result[var] = [col_index]
        return result


    def get_dict(self) -> dict:
        self.failures = []
        region = self._get_region()
        variable_unit_map = self._process_unit_columns()

        # template = collections.OrderedDict()
        template = dict()
        no_main_subject_warning = ''
        if self.main_subject_index == 0:
            no_main_subject_warning == '  # FIX ME'
            self.failures.append('No main subject column')
        template['item'] = f'=item[{to_letter_column(self.main_subject_index)}, $row, "main subject"]{no_main_subject_warning}'
        template['property'] = f'=item[$col, {self.header_index+1}, "property"]'
        template['value'] = '=value[$col, $row]'

        if self.units_indices.shape[0] > 0:
            if None in variable_unit_map:
                unit_cols = variable_unit_map[None]
                if len(unit_cols) > 1:
                    cells = ', '.join([f'value[{to_letter_column(col)}, $row]' for col in unit_cols])
                    value = f'=get_item(concat({cells} , ", "), "unit")'
                else:
                    col = unit_cols[0]
                    value = f'=item[{to_letter_column(col)}, $row, "unit"]'
                template['unit'] = value
            else:
                template['unit'] = f'=item[$col, {self.unit_index+1}, "unit"]'

        qualifier = []
        qualifier.append(self._get_time())
        qualifier.append(self._get_dataset())
        qualifier += self._get_admins()
        qualifier += self._get_coordinate()
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
    # input_file = '/home/kyao/Shared/Datamart/data/D3M/GDL-AreaData370-Ethiopia-description.xlsx'
    # input_file = '/home/kyao/dev/t2wml-projects/projects/aid/csv/aid worker security_incidents2020-06-22.xlsx'
    input_file = '/home/ktyao/dev/dsbox/t2wml-projects/aid_worker/aid worker security_incidents2020-06-22-annotated.xlsx'
    sheet = pd.read_excel(input_file, header=None)
    sheet.iloc[1, :] = sheet.iloc[1, :].fillna(method='ffill')
    to_t2wml = ToT2WML(sheet, 'Qawsd')
    print(to_t2wml.get_yaml())
