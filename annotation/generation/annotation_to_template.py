import pandas as pd
import logging
import os
from collections import defaultdict
from annotation.utility import Utility

_logger = logging.getLogger(__name__)
TYPE_MAP_DICT = {"string": "String", "number": "Quantity", "year": "Time", "month": "Time", "day": "Time",
                 "date": "Time"}

# kyao
# Only add one location qualifier until datamart-api can handle multiple locations. 31 July 2020.
ADDITIONAL_QUALIFIER_MAP = {
    # ("lat", "lon", "latitude", "longitude"): {"Attribute": "location", "Property": "P276"},
    # ("country",): {"Attribute": "country", "Property": "P17"},
    # ("admin1",): {"Attribute": "located in the first-level administrative country subdivision",
    #               "Property": "P2006190001"},
    # ("admin2",): {"Attribute": "located in the second-level administrative country subdivision",
    #               "Property": "P2006190002"},
    # ("admin3",): {"Attribute": "located in the third-level administrative country subdivision",
    #               "Property": "P2006190003"},
    ("country", "admin1", "admin2", "admin1"): {"Attribute": "located in the administrative territorial entity",
                                                "Property": "P131"},
}


def generate_template_from_df(input_df: pd.DataFrame, dataset_id: str = None) -> dict:
    """
    function used for datamart annotation batch mode, return a dict of dataFrame instead of output a xlsx file
    """
    utility = Utility()

    if dataset_id is None:
        dataset_id = input_df.iloc[0, 0]

    # TODO fix this set index reset index business
    input_df = input_df.reset_index()
    # updated 2020.7.22: it is possible that header is not at row 7, so we need to search header row if exist
    header_row, data_row = utility.find_data_start_row(input_df)

    if 'tag' in input_df.iloc[:7, 0]:
        annotation_rows = list(range(1, 7)) + [header_row]
    else:
        annotation_rows = list(range(1, 6)) + [header_row]
    content_rows = list(range(data_row, len(input_df)))

    input_df = input_df.set_index(0)
    annotation_part = input_df.iloc[annotation_rows].fillna("")
    content_part = input_df.iloc[content_rows]

    # start generate dataframe for templates
    dataset_df = _generate_dataset_tab(dataset_id)
    attribute_df = _generate_attributes_tab(dataset_id, annotation_part)
    unit_df = _generate_unit_tab(dataset_id, content_part, annotation_part)
    extra_df, wikifier_df1 = _process_main_subject(dataset_id, content_part, annotation_part, data_row)
    wikifier_df2 = _generate_wikifier_part(content_part, annotation_part, data_row)
    wikifier_df = pd.concat([wikifier_df1, wikifier_df2])

    output_df_dict = {
        'dataset_file': dataset_df,
        'attributes_file': attribute_df,
        'units_file': unit_df,
        "extra_edges": extra_df,
        "Wikifier_t2wml": wikifier_df,
        "wikifier": None,
        "qualifiers": None,
    }
    return output_df_dict


def generate_template(input_path: str, output_path: str, dataset_id: str = None) -> None:
    """
    generate the template xlsx file from the input xlsx file
    :param dataset_id:
    :param input_path:
    :param output_path:
    :return:
    """
    input_df = pd.read_excel(input_path, index_col=0, header=None)
    output_df_dict = generate_template_from_df(input_df, dataset_id=dataset_id)
    output_folder = output_path[:output_path.rfind("/")]
    os.makedirs(output_folder, exist_ok=True)
    save_template_file(output_df_dict, output_path)


def save_template_file(output_df_dict: dict, output_path: str) -> None:
    with pd.ExcelWriter(output_path) as writer:
        output_df_dict["dataset_file"].to_excel(writer, sheet_name='Dataset', index=False)
        output_df_dict["attributes_file"].to_excel(writer, sheet_name='Attributes', index=False)
        output_df_dict["units_file"].to_excel(writer, sheet_name='Units', index=False)
        output_df_dict["extra_edges"].to_excel(writer, sheet_name="Extra Edges", index=False)
        output_df_dict["Wikifier_t2wml"].to_excel(writer, sheet_name="Wikifier_t2wml", index=False)


def _generate_dataset_tab(dataset_id: str) -> pd.DataFrame:
    """
    A sample dataset file looks like: here {dataset_id} = "aid-security"
    node1	        label	    node2	                id
    Qaid-security	P31	        Q1172284	            aid-security-P31
    Qaid-security	label	    aid-security dataset	aid-security-label
    Qaid-security	P1476	    aid-security dataset	aid-security-P1476
    Qaid-security	description	aid-security dataset	aid-security-description
    Qaid-security	P2699	    aid-security	        aid-security-P2699
    Qaid-security	P1813	    aid-security	        aid-security-P1813

    :param dataset_id: input dataset id
    :return:
    """
    dataset_id_df_list = []

    dataset_labels = ["P31", "label", "P1476", "description", "P2699", "P1813"]
    dataset_node2s = ["Q1172284", "{} dataset".format(dataset_id), "{} dataset".format(dataset_id),
                      "{} dataset".format(dataset_id), dataset_id, dataset_id]
    for label, node2 in zip(dataset_labels, dataset_node2s):
        dataset_id_df_list.append({"dataset": dataset_id, "label": label, "node2": node2})

    dataset_df = pd.DataFrame(dataset_id_df_list)
    return dataset_df


def _generate_attributes_tab(dataset_id: str, annotation_part: pd.DataFrame) -> pd.DataFrame:
    """
        codes used to generate the template attribute tab
        1. add for columns with role = variable or role = qualifier.
    """
    attributes_df_list = []
    seen_attributes = {}
    for i in range(annotation_part.shape[1]):
        each_col_info = annotation_part.iloc[:, i]
        role_info = each_col_info["role"].split(";")
        role_lower = role_info[0].lower()

        # update 2020.7.29, add an extra qualifier for string main subject condition
        if role_lower == "main subject" and each_col_info["type"].lower() == "string":
            all_column_types = set(annotation_part.T['type'].unique())
            for types, edge_info in ADDITIONAL_QUALIFIER_MAP.items():
                if len(set(types).intersection(all_column_types)) > 0:
                    attributes_df_list.append({"Attribute": edge_info["Attribute"],
                                               "Property": edge_info["Property"], "Role": "qualifier",
                                               "Relationship": "", "type": "WikibaseItem",
                                               "label": edge_info["Attribute"],
                                               "description": edge_info["Attribute"]})
            continue

        elif role_lower in {"variable", "qualifier"}:
            # if ";" exists, we need to use those details on variables
            if len(role_info) > 1:
                relationship = role_info[1]
            # otherwise apply this variable / qualifier for all by give empty cell
            else:
                relationship = ""
            attribute = each_col_info["header"]
            role_type = each_col_info["type"].lower()
            if role_type == "":
                continue

            if role_type not in TYPE_MAP_DICT:
                raise ValueError("Column type {} for column {} is not valid!".format(role_type, i))
            data_type = TYPE_MAP_DICT[each_col_info["type"]]
            label = "{}".format(attribute) if not each_col_info['name'] else each_col_info['name']
            description = "{} column in {}".format(role_lower, dataset_id) if not each_col_info['description'] \
                else each_col_info['description']
            tag = each_col_info['tag'] if 'tag' in each_col_info else ""

            # qualifier and variables have been deduplicated already in validation. Now if anything is repeating,
            # it is meant to be same.
            if attribute not in seen_attributes:
                attributes_df_list.append({"Attribute": attribute, "Property": "", "Role": role_lower,
                                           "Relationship": relationship, "type": data_type,
                                           "label": label, "description": description, "tag": tag})
                seen_attributes[attribute] = 1

    if len(attributes_df_list) == 0:
        attributes_df = pd.DataFrame(columns=['Attribute', 'Property', 'label', 'description'])
    else:
        attributes_df = pd.DataFrame(attributes_df_list)

    return attributes_df


def _generate_unit_tab(dataset_id: str, content_part: pd.DataFrame, annotation_part: pd.DataFrame) -> pd.DataFrame:
    """
        codes used to generate the template unit tab
        1. list all the distinct units defined in the units row
        2. If there are columns with role == unit, also add them

        The output will have 2 columns like:
        Q-Node can be empty or user specified nodes, if automatically generated from this script,
        it will always be empty and with be generated in wikify_datamart_units_and_attributes.py
        Unit	    Q-Node
        person	    ""
    """
    unit_df_list = []
    unit_cols = defaultdict(list)
    units_set = set()

    for i in range(annotation_part.shape[1]):
        each_col_info = annotation_part.iloc[:, i]

        role = each_col_info["role"].lower()
        # if role is unit, record them
        if len(role) >= 4 and role[:4] == "unit":
            if role == "unit":
                unit_cols[""].append(i)
            # update 2020.7.24, now allow unit only corresponding to specific variables
            else:
                target_variables = role[role.rfind(";") + 1:]
                for each_variable in target_variables.split("|"):
                    unit_cols[each_variable].append(i)

        # add units defined in unit
        if each_col_info['unit'] != "":
            units_set.add(each_col_info['unit'])

    if len(unit_cols) > 0:
        for each_variable_units in unit_cols.values():
            units_set.update(content_part.iloc[:, each_variable_units].agg(", ".join, axis=1).unique())

    # sort for better index for human vision
    for each_unit in sorted(list(units_set)):
        unit_df_list.append({"Unit": each_unit, "Q-Node": ""})

    if len(unit_df_list) == 0:
        unit_df = pd.DataFrame(columns=['Unit', 'Q-Node'])
    else:
        unit_df = pd.DataFrame(unit_df_list)

    return unit_df


def _process_main_subject(dataset_id: str, content_part: pd.DataFrame, annotation_part: pd.DataFrame, data_row):
    col_offset = 1
    wikifier_df_list = []
    extra_df_list = []
    created_node_ids = set()
    for i in range(annotation_part.shape[1]):
        each_col_info = annotation_part.iloc[:, i]
        role = each_col_info["role"].lower()
        if role == "main subject":
            allowed_types = {"string", "country", "admin1", "admin2", "admin3"}
            each_col_info = annotation_part.iloc[:, i]
            type_ = each_col_info["type"].lower()
            main_subject_annotation = annotation_part.iloc[:, i]

            # generate wikifier file and extra edge file for main subjects when type == string
            if type_ == "string":
                for row, each in enumerate(content_part.iloc[:, i]):
                    label = str(each).strip()
                    node = "Q{}_{}_{}".format(dataset_id, each_col_info["header"], label) \
                        .replace(" ", "_").replace("-", "_")

                    # wikifier part should always be updated, as column/row is specified for each cell
                    wikifier_df_list.append(
                        {"column": i + col_offset, "row": row + data_row, "value": label,
                         "context": "main subject", "item": node})

                    # update 2020.7.24, not create again if exist
                    if node in created_node_ids:
                        continue
                    created_node_ids.add(node)

                    labels = ["label", "description", "P31"]
                    node2s = ["{} {}".format(main_subject_annotation["header"], label),
                              main_subject_annotation["description"], "Q35120"]
                    for each_label, each_node2 in zip(labels, node2s):
                        id_ = "{}-{}".format(node, each_label)
                        extra_df_list.append({"id": id_, "node1": node, "label": each_label, "node2": each_node2})

            elif type_ not in allowed_types:
                raise ValueError("{} is not a legal type among {{{}}}!".format(type_, allowed_types))
            # only one main subject so no need to continue
            break

    if len(extra_df_list) == 0:
        extra_df = pd.DataFrame(columns=['id', 'node1', 'label', 'node2'])
    else:
        extra_df = pd.DataFrame(extra_df_list)

    if len(wikifier_df_list) == 0:
        wikifier_df = pd.DataFrame(columns=['column', 'row', 'value', 'context', "item"])
    else:
        wikifier_df = pd.DataFrame(wikifier_df_list)

    return extra_df, wikifier_df


def _generate_wikifier_part(content_part: pd.DataFrame, annotation_part: pd.DataFrame, data_row):
    # generate wikifier file for all columns that have type == country, admin1, admin2, or admin3
    # TODO: set country wikifier and ethiopia wikifier to be a service
    wikifier_df_list = []
    target_cols = []
    run_ethiopia_wikifier = False
    has_country_column = False
    new_wikifier_roles = {"location": "", "main subject": "main subject"}
    data_type_need_wikifier = {"admin1", "admin2", "admin3"}
    wikifier_column_metadata = []  # for column metadata in wikifier output
    col_offset = 1
    for i in range(annotation_part.shape[1]):
        each_col_info = annotation_part.iloc[:, i]
        if each_col_info["role"] in new_wikifier_roles:
            if each_col_info["type"] == "country":
                has_country_column = True
                # use country wikifier
                context = "main subject" if each_col_info["role"] == "main subject" else each_col_info["type"]
                column_metadata = {"context": context}
                wikifier_df_list.extend(run_wikifier(
                    input_df=content_part, target_col=i,
                    return_type="list", wikifier_type="country", column_metadata=column_metadata))

                if "ethiopia" in [each.lower() for each in content_part.iloc[:, i].dropna().unique()]:
                    run_ethiopia_wikifier = True

            if each_col_info["type"] in data_type_need_wikifier:
                target_cols.append(i)
                each_metadata = {}
                if each_col_info["role"] == "location":
                    each_metadata["context"] = each_col_info["type"]
                else:
                    each_metadata["context"] = new_wikifier_roles[each_col_info["role"]]
                wikifier_column_metadata.append(each_metadata)

    # if no country column exists, assume the country is Ethiopia
    if not has_country_column:
        run_ethiopia_wikifier = True

    if run_ethiopia_wikifier and target_cols:

        # get target columns to run with wikifier

        # Handle non-numeric column names
        # target_df = content_part.iloc[:, target_cols].reset_index().drop(columns=[0])
        target_df = content_part.iloc[:, target_cols].reset_index()
        target_df = target_df.drop(columns=[target_df.columns[0]])

        # run wikifier on each column
        for i in range(len(target_cols)):
            # for each part, run wikifier and add it the wikifier file
            wikifier_df_list.extend(run_wikifier(input_df=target_df, target_col=i, wikifier_type="ethiopia",
                                                 col_offset=col_offset + target_cols[i] - i, row_offset=data_row,
                                                 column_metadata=wikifier_column_metadata[i]
                                                 )
                                    )

    if len(wikifier_df_list) == 0:
        wikifier_df = pd.DataFrame(columns=['column', 'row', 'value', 'context', "item"])
    else:
        wikifier_df = pd.DataFrame(wikifier_df_list)
    return wikifier_df


def run_wikifier(input_df: pd.DataFrame, target_col: int, wikifier_type: str, return_type: str = "list",
                 col_offset=0, row_offset=0, column_metadata: dict = None):
    wikifier_df_list = []
    if column_metadata is None:
        column_metadata = {}
    if wikifier_type == "country":
        from annotation.generation.country_wikifier import DatamartCountryWikifier
        wikified_result = DatamartCountryWikifier(). \
            wikify(input_df.iloc[:, target_col].dropna().unique().tolist())
        for label, node in wikified_result.items():
            each_row = {"column": "", "row": "", "value": label, "context": column_metadata.get("context", ""),
                        "item": node}
            if node and label and each_row not in wikifier_df_list:
                wikifier_df_list.append(each_row)

    elif wikifier_type == "ethiopia":
        from annotation.generation.ethiopia_wikifier import EthiopiaWikifier
        wikifier = EthiopiaWikifier()
        input_col_name = input_df.columns[target_col]
        output_col_name = "{}_wikifier".format(input_col_name)
        wikifier_res = wikifier. \
            produce(input_df=input_df, target_column=input_col_name, column_metadata=column_metadata). \
            fillna("")
        # wrap to t2wml wikifier format
        for row_number, each_row in wikifier_res.iterrows():
            label = each_row[input_col_name]
            node = each_row[output_col_name]
            if node != "" and label != "":
                # to prevent duplicate names with different nodes, we need to create column and row number here
                wikifier_df_list.append(
                    {"column": col_offset + target_col, "row": row_number + row_offset, "value": label,
                     "context": column_metadata.get("context", ""), "item": node})
    else:
        raise ValueError("Unsupport wikifier type!")

    if return_type == "list":
        return wikifier_df_list
    else:
        return pd.DataFrame(wikifier_df_list)
