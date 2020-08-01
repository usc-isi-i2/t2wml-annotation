import pandas as pd
import os
import string
import csv
import copy
import typing

from annotation.generation.annotation_to_template import run_wikifier as get_wikifier_result
from pathlib import Path
from collections import defaultdict

"""
How to use:
Step 1: load file
b. call "load_xlsx" send with one file path

Step 2: generate file
call "generate" after you loaded the file 

the output folder location and column name config (optional) if needed
if column name config not given or partial not given the system will use:

Example file:
https://docs.google.com/spreadsheets/d/1NuTmRIxpy460S4CRdP6XORKFILssOby_RxiFbONXwv0/edit#gid=756069733


For attribute file:
Assume "Property" column exist and it is the node column
Assume "Attribute" column exist and it is the label column

For unit file:
Assume "Unit" column exist and it is the node column
Assume "Q-Node" column exist and it is the label column
"""
stop_punctuation = string.punctuation
TRANSLATOR = str.maketrans(stop_punctuation, ' ' * len(stop_punctuation))


# deprecated! not use
def load_csvs(dataset_file: str, attributes_file: str, units_file: str):
    loaded_file = {}
    files = [dataset_file, attributes_file, units_file]
    file_type = ["dataset_file", "attributes_file", "units_file"]
    for each_file, each_file_type in zip(files, file_type):
        if each_file:
            if not os.path.exists(each_file):
                raise ValueError("{} {} not exist!".format(each_file_type, each_file))
            loaded_file[each_file_type] = pd.read_csv(each_file)
    return loaded_file


def load_xlsx(input_file: str, sheet_name_config: dict = None):
    loaded_file = {}
    sheet_names = pd.ExcelFile(input_file).sheet_names
    if not sheet_name_config:
        sheet_name_config = {"dataset_file": "Dataset",
                             "attributes_file": "Attributes",
                             "units_file": "Units",
                             "extra_edges": "Extra Edges"
                             }
    for k, v in sheet_name_config.items():
        if v not in sheet_names:
            raise ValueError("Sheet name {} used for {} does not found!".format(v, k))
        loaded_file[k] = pd.read_excel(input_file, v)

    optional_sheet_name_config = {
        "wikifier": "Wikifier",
        "qualifiers": "Qualifiers",
        "Wikifier_t2wml": "Wikifier_t2wml",
        "Wikifier Columns": "Wikifier Columns"
    }
    for k, v in optional_sheet_name_config.items():
        if v not in sheet_names:
            loaded_sheet = None
        else:
            loaded_sheet = pd.read_excel(input_file, v)
        loaded_file[k] = loaded_sheet

    return loaded_file


def generate(loaded_file: dict, output_path: str = ".", column_name_config=None, to_disk=True,
             datamart_properties_file: str = None, dataset_qnode: str = None, dataset_id: str = None,
             debug: bool = False,
             ) -> typing.Optional[dict]:
    """
    The main entry function for generating datamart files from template input,
    base on input parameter `to_disk`, the output can be None or dict of dataframe
    """
    if column_name_config is None:
        column_name_config = {}
    if "attributes_file_node_column_name" not in column_name_config:
        column_name_config["attributes_file_node_column_name"] = "Property"
    if "attributes_file_node_label_column_name" not in column_name_config:
        column_name_config["attributes_file_node_label_column_name"] = "Attribute"
    if "unit_file_node_column_name" not in column_name_config:
        column_name_config["unit_file_node_column_name"] = "Q-Node"
    if "unit_file_node_label_column_name" not in column_name_config:
        column_name_config["unit_file_node_label_column_name"] = "Unit"

    if len(loaded_file["dataset_file"]["dataset"].unique()) > 1:
        raise ValueError("One dataset file should only contains 1 dataset ID in `dataset` column.")

    if loaded_file["wikifier"] is not None:
        extra_wikifier_dict = get_wikifier_part(loaded_file["wikifier"])
    else:
        extra_wikifier_dict = {}

    # update 2020.7.22: accept user specified dataset id if given
    if dataset_qnode is None:
        dataset_qnode = loaded_file["dataset_file"]["dataset"].iloc[0]

    if dataset_id is None:
        dataset_id = loaded_file["dataset_file"]["dataset"].iloc[0]

    # generate files
    memo = defaultdict(dict)
    kgtk_properties_df = _generate_KGTK_properties_file(loaded_file["attributes_file"],
                                                        loaded_file["qualifiers"],
                                                        dataset_qnode, dataset_id,
                                                        memo, column_name_config["attributes_file_node_column_name"],
                                                        column_name_config["attributes_file_node_label_column_name"])

    kgtk_variables_df = _generate_KGTK_variables_file(loaded_file["attributes_file"],
                                                      dataset_qnode, dataset_id, memo,
                                                      column_name_config["attributes_file_node_column_name"],
                                                      column_name_config["attributes_file_node_label_column_name"])

    kgtk_units_df = _generate_KGTK_units_file(loaded_file["units_file"], dataset_qnode, memo,
                                              column_name_config["unit_file_node_column_name"],
                                              column_name_config["unit_file_node_label_column_name"])

    wikifier_df = _generate_wikifier_file(memo, extra_wikifier_dict)
    if loaded_file["Wikifier_t2wml"] is not None:
        wikifier_df = pd.concat([wikifier_df, loaded_file["Wikifier_t2wml"]])

    dataset_df = _generate_dataset_file(loaded_file["dataset_file"])
    extra_edges_df = _generate_extra_edges_file(loaded_file["extra_edges"], memo)

    output_files = {"kgtk_properties.tsv": kgtk_properties_df,
                    "kgtk_variables.tsv": kgtk_variables_df,
                    "kgtk_units.tsv": kgtk_units_df,
                    "wikifier.csv": wikifier_df,
                    "extra_edges.tsv": extra_edges_df,
                    "dataset.tsv": dataset_df}

    # save to disk if required or running in debug mode
    if to_disk or debug:
        os.makedirs(output_path, exist_ok=True)
        for each_file_name, each_file in output_files.items():
            output_file_path = os.path.join(output_path, each_file_name)
            if each_file_name.endswith(".csv"):
                each_file.to_csv(output_file_path, index=False)
            elif each_file_name.endswith(".tsv"):
                each_file.to_csv(output_file_path, sep='\t', index=False, quoting=csv.QUOTE_NONE)

    if not to_disk:
        return output_files


def _generate_KGTK_properties_file(input_df: pd.DataFrame, qualifier_df: pd.DataFrame,
                                   dataset_q_node: str, dataset_id: str, memo: dict,
                                   node_column_name="Property", node_label_column_name="Attribute",
                                   qualifier_column_name="Qualifiers") -> pd.DataFrame:
    """
        sample format for each property (totally 3 rows)
        Please note that data type may change (to String, Date) base on the given input template file
            id	                            node1	            label	    node2
        0   Paid-security-002-data_type	    Paid-security-002	data_type	Quantity
        1   Paid-security-002-P31	        Paid-security-002	P31	        Q18616576
        2   Paid-security-002-label	        Paid-security-002	label	    UN

    :return: kgtk format property dataframe
     """
    node_number = 1
    output_df_list = []
    input_df = input_df.fillna("")
    has_relationship = 'Relationship' in input_df.columns and 'Role' in input_df.columns

    for _, each_row in input_df.iterrows():
        node_number += 1
        if has_relationship:
            role = each_row["Role"].upper()
        else:
            role = ""
        if each_row[node_column_name] == "":
            node_label = to_kgtk_format_string(each_row[node_label_column_name])
            node_id = _generate_p_nodes(role, dataset_q_node, node_number, memo, each_row['Attribute'])

            # get type if specified
            if "type" in each_row:
                value_type = each_row["type"]
            else:
                value_type = "Quantity"

            labels = ["data_type", "P31", "label"]
            node2s = [value_type, "Q18616576", node_label]
            for i in range(3):
                id_ = "{}-{}".format(node_id, labels[i])
                output_df_list.append({"id": id_, "node1": node_id, "label": labels[i], "node2": node2s[i]})
        else:
            node_id = each_row[node_column_name]

        # add to memo for future use
        memo["property"][node_id] = each_row[node_label_column_name]
        if "Role" in each_row:
            memo["property_role"][node_id] = each_row["Role"].lower()

    # add qualifier part if we have
    if qualifier_df is not None:
        qualifier_df = qualifier_df.fillna("")
        for _, each_row in qualifier_df.iterrows():
            node_number += 1
            if each_row[node_column_name] == "":
                node_id = _generate_p_nodes("QUALIFIER", dataset_q_node, node_number, memo, each_row["Attribute"])
                memo["qualifier_target_nodes"][each_row[qualifier_column_name]] = memo["property_name_to_id"][
                    each_row[node_label_column_name]]
                memo["qualifier_name_to_id"][each_row[qualifier_column_name]] = node_id
                memo["property"][node_id] = each_row[qualifier_column_name]
                labels = ["data_type", "P31", "label"]
                node2s = ["String", "Q18616576", to_kgtk_format_string(each_row[qualifier_column_name])]
                for i in range(3):
                    id_ = "{}-{}".format(node_id, labels[i])
                    output_df_list.append({"id": id_, "node1": node_id, "label": labels[i], "node2": node2s[i]})
            else:
                memo["property"][each_row[node_column_name]] = each_row[qualifier_column_name]
                memo["qualifier_name_to_id"][each_row[qualifier_column_name]] = each_row[node_column_name]
                memo["qualifier_target_nodes"][each_row[qualifier_column_name]] = memo["property_name_to_id"][
                    each_row[node_label_column_name]]

    # get output
    output_df = pd.DataFrame(output_df_list)
    # in case of empty df
    if output_df.shape == (0, 0):
        output_df = pd.DataFrame(columns=['id', 'node1', 'label', 'node2'])
    return output_df


def _generate_KGTK_variables_file(input_df: pd.DataFrame, dataset_q_node: str, dataset_id: str, memo: dict,
                                  node_column_name="Property", node_label_column_name="Attribute"):
    """
        sample format for each variable, totally 10 + n (n is the count of related qualifiers) rows
            "id"                                 "node1"        "label"          "node2"
        0   QVARIABLE-OECD-002-label             QVARIABLE-002   label           "GDP per capita"
        1   QVARIABLE-OECD-002-P1476             QVARIABLE-002   P1476           "GDP per capita"
        2   QVARIABLE-OECD-002-description       QVARIABLE-002   description     "GDP per capita variable in OECD"
        3   QVARIABLE-OECD-002-P31-1             QVARIABLE-002   P31             Q50701
        4   QVARIABLE-OECD-002-P2006020002-P248  QVARIABLE-002   P2006020002     P585
        5   QVARIABLE-OECD-002-P2006020002-P248  QVARIABLE-002   P2006020002     P248
        6   QVARIABLE-OECD-002-P1687-1           QVARIABLE-002   P1687           PVARIABLE-OECD-002
        7   QVARIABLE-OECD-002-P2006020004-1     QVARIABLE-002   P2006020004     QOECD
        8   QVARIABLE-OECD-002-P1813             QVARIABLE-002   P1813           "gdp_per_capita"
        9   QVARIABLE-OECD-P2006020003-QOECD002  QVARIABLE       P2006020003     QOECD-002
        -------------------------------------------------
        10   QVARIABLE-OECD-P2006020002-PQUALIFIER-OECD-101  QVARIABLE       P2006020003     PQUALIFIER-OECD-101
        11   QVARIABLE-OECD-P2006020002-PQUALIFIER-OECD-102  QVARIABLE       P2006020003     PQUALIFIER-OECD-102
    """
    node_number = 1
    output_df_list = []
    short_name_memo = set()
    input_df = input_df.fillna("")

    all_qualifier_properties = []
    for node, role in memo["property_role"].items():
        if role == "qualifier":
            all_qualifier_properties.append(node)

    has_relationship = 'Relationship' in input_df.columns and 'Role' in input_df.columns

    for _, each_row in input_df.iterrows():
        if has_relationship:
            role = each_row["Role"].upper()
        else:
            role = ""

        # not add QUALIFIER to variables tab
        if has_relationship and role == "QUALIFIER":
            continue

        target_properties = []
        # update 2020.7.22: consider role and relationship for new template file
        if has_relationship:
            relations = each_row['Relationship']
            # qualifier should not have qualifier properties
            if each_row['Role'].lower() != "qualifier":
                if relations == "":
                    target_properties = all_qualifier_properties
                else:
                    for each_relation in relations.slipt("|"):
                        if each_relation not in memo["property_name_to_id"]:
                            raise ValueError(
                                "Annotation specify variable {} not exist in input data.".format(each_relation))
                        target_properties.append(memo["property_name_to_id"][each_relation])

        node_number += 1
        if each_row[node_column_name] == "":
            # update 2020.7.23, also add role for P nodes
            p_node_id = _generate_p_nodes(role, dataset_q_node, node_number, memo, each_row["Attribute"])
        else:
            p_node_id = each_row[node_column_name]

        # update 2020.7.22: change to add role in Q node id
        q_node_id = _generate_q_nodes(role, dataset_q_node, node_number)
        memo["variable"][q_node_id] = each_row[node_label_column_name]

        fixed_labels = ["label", "P1476", "description",  # 1-3
                        "P31", "P2006020002", "P2006020002",  # 4-6
                        "P1687", "P2006020004", "P1813",  # 7-9
                        "P2006020003"]
        labels = fixed_labels + len(target_properties) * ["P2006020002"]
        node2s = [to_kgtk_format_string(each_row[node_label_column_name]),  # 1
                  to_kgtk_format_string(each_row[node_label_column_name]),  # 2
                  to_kgtk_format_string("{} in {}".format(each_row[node_label_column_name], dataset_id)),  # 3
                  "Q50701", "P585", "P248",  # 4(Q50701 = variable), 5(P585 = Point in time), 6(P249 = stated in)
                  p_node_id,  # 7
                  dataset_q_node,  # 8
                  get_short_name(short_name_memo, each_row[node_label_column_name]),  # 9
                  q_node_id  # 10
                  ] + target_properties
        node1s = [q_node_id] * (len(fixed_labels) - 1) + [dataset_q_node] + [q_node_id] * len(target_properties)

        # add those nodes
        for i, each_label in enumerate(labels):
            id_ = _generate_edge_id(node1s[i], labels[i], node2s[i])
            output_df_list.append({"id": id_, "node1": node1s[i], "label": labels[i], "node2": node2s[i]})

    # get output
    output_df = pd.DataFrame(output_df_list)
    # in case of empty df
    if output_df.shape == (0, 0):
        output_df = pd.DataFrame(columns=['id', 'node1', 'label', 'node2'])
    return output_df


def _generate_KGTK_units_file(input_df: pd.DataFrame, dataset_q_node: str, memo: dict, node_column_name="Q-Node",
                              node_label_column_name="Unit") -> pd.DataFrame:
    """
        sample format for each unit (totally 2 rows)
        id	                        node1	            label	node2
    0   QUNIT-aid-security-U002-label	Qaid-security-U002	label	person
    1   QUNIT-aid-security-U002-P31	    Qaid-security-U002	P31	    Q47574

    :return:
    """
    node_number = 1
    count = 0
    output_df_dict = {}
    input_df = input_df.fillna("")

    for _, each_row in input_df.iterrows():
        node_number += 1
        if each_row[node_column_name] == "":
            # update 2020.7.22: change to use QUNIT* instead of Q*
            node_id = _generate_q_nodes("UNIT", dataset_q_node, node_number)
            labels = ["label", "P31"]
            node2s = [to_kgtk_format_string(each_row[node_label_column_name]), "Q47574"]
            memo["unit"][node_id] = each_row[node_label_column_name]
            for i in range(2):
                id_ = _generate_edge_id(node_id, labels[i], node2s[i])
                output_df_dict[count] = {"id": id_, "node1": node_id, "label": labels[i], "node2": node2s[i]}
                count += 1
        else:
            memo["unit"][each_row[node_column_name]] = each_row[node_label_column_name]
    # get output
    output_df = pd.DataFrame.from_dict(output_df_dict, orient="index")
    # in case of empty df
    if output_df.shape == (0, 0):
        output_df = pd.DataFrame(columns=['id', 'node1', 'label', 'node2'])
    return output_df


def _generate_wikifier_file(memo, extra_wikifier_dict):
    """
        generate the wikifier part from template(those properties, variables, units generated in above functions)
        Sample file looks like:
            column	row	    value	    context	    item
        0	  ""     ""     UN	        property	Paid-security-002
        1	  ""     ""     INGO	    property	Paid-security-003
        2	  ""     ""          LNGO/NRCS	property	Paid-security-004
        3	  ""     ""          ICRC	    property	Paid-security-005
        4	  ""     ""        UN	        variable	Qaid-security-002
        5	  ""     ""          INGO	    variable	Qaid-security-003
        6     ""     ""          person	    unit	    Qaid-security-U002
    """
    output_df_list = []
    for memo_type, each_memo in memo.items():
        if memo_type in {"property", "unit", "variable"}:
            for node, label in each_memo.items():
                output_df_list.append({"column": "", "row": "", "value": label, "context": memo_type, "item": node})
                # for those specific alias of wikifier names
                combo = (label, memo_type)
                if combo in extra_wikifier_dict:
                    output_df_list.append(
                        {"column": "", "row": "", "value": extra_wikifier_dict[combo], "context": memo_type,
                         "item": node})

    # get output
    output_df = pd.DataFrame(output_df_list)
    return output_df


def _generate_dataset_file(input_df: pd.DataFrame):
    """
    A sample dataset file looks like:
    node1	        label	    node2	                id
    Qaid-security	P31	        Q1172284	            aid-security-P31
    Qaid-security	label	    aid-security dataset	aid-security-label
    Qaid-security	P1476	    aid-security dataset	aid-security-P1476
    Qaid-security	description	aid-security dataset	aid-security-description
    Qaid-security	P2699	    aid-security	        aid-security-P2699
    Qaid-security	P1813	    aid-security	        aid-security-P1813
    :return:
    """
    output_df = copy.deepcopy(input_df)
    ids = []
    for _, each_row in output_df.iterrows():
        ids.append("{}-{}".format(each_row["dataset"], each_row["label"]))
    output_df['id'] = ids
    output_df["dataset"] = output_df['dataset'].apply(lambda x: "Q" + x)
    output_df = output_df.rename(columns={"dataset": "node1"})
    # check double quotes
    output_df = _check_double_quotes(output_df, check_content_startswith=True)
    return output_df


def _generate_extra_edges_file(input_df: pd.DataFrame, memo: dict):
    qualifier_extra_edges_list = []
    if "qualifier_target_nodes" in memo:
        for k, v in memo['qualifier_target_nodes'].items():
            qualifier_extra_edges_list.append({"id": "", "node1": v, "label": "P2006020002",
                                               "node2": memo["qualifier_name_to_id"][k]})

    output_df = pd.concat([input_df, pd.DataFrame(qualifier_extra_edges_list)])
    # check double quotes
    output_df = _check_double_quotes(output_df, label_types={"label", "description"})
    return output_df


# update 2020.7.24, add support of run wikifier and record t2wml wikifier file in template
def run_wikifier(input_folder_path: str, wikifier_columns_df: pd.DataFrame, template_output_path: str):
    """
    run wikifier on all table files(csv, xlsx, xls) and add the new wikifier results to "wikifier.csv" file
    :param input_folder_path:
    :param wikifier_columns_df:
    :param template_output_path:
    :return:
    """
    new_wikifier_df_list = []
    input_data = []

    for each_file in os.listdir(input_folder_path):
        if each_file.startswith("~") or each_file.startswith("."):
            continue

        each_file = os.path.join(input_folder_path, each_file)
        if each_file.endswith(".csv"):
            input_data.append(pd.read_csv(each_file, header=None))
        elif each_file.endswith(".xlsx") or each_file.endswith("xls"):
            for each_sheet in get_sheet_names(each_file):
                input_data.append(pd.read_excel(each_file, each_sheet, header=None))

    for each_df in input_data:
        each_df = each_df.fillna("")

        # get only data part that need to be parsed
        for _, each_row in wikifier_columns_df.iterrows():
            target_column_number = ord(each_row['Columns']) - ord("A")
            start_row, end_row = each_row["Rows"].split(":")
            start_row = int(start_row) - 1
            if end_row == "":
                end_row = len(each_df)
            else:
                end_row = int(end_row)

            if target_column_number >= each_df.shape[1] or end_row >= each_df.shape[0]:
                print("Required to wikify on column No.{} and end row at {} but the input dataframe shape is only {}").\
                    format(target_column_number, end_row, each_df.shape)
                continue

            each_df = each_df.iloc[start_row:end_row, :]
            # run wikifier
            new_wikifier_df_list.extend(get_wikifier_result(input_df=each_df, target_col=target_column_number,
                                                            wikifier_type="country"))
            wikified_values = set([each["value"] for each in new_wikifier_df_list])
            remained_df_part = each_df[~each_df.iloc[:, target_column_number].isin(wikified_values)]
            new_wikifier_df_list.extend(get_wikifier_result(input_df=remained_df_part, target_col=target_column_number,
                                                            wikifier_type="ethiopia"))

    new_wikifier_df = pd.DataFrame(new_wikifier_df_list)
    # combine the previous wikifier file if exists
    output_wikifier_file_path = os.path.join(template_output_path, "wikifier.csv")
    if os.path.exists(output_wikifier_file_path):
        new_wikifier_df = pd.concat([pd.read_csv(output_wikifier_file_path), new_wikifier_df])

    # save to disk
    new_wikifier_df.to_csv(output_wikifier_file_path, index=False)


def get_short_name(short_name_memo, input_str):
    words_processed = str(input_str).lower().translate(TRANSLATOR).split()
    short_name = "_".join(words_processed)
    if short_name[0].isnumeric():
        short_name = "_" + short_name
    i = 0
    while short_name in short_name_memo:
        i += 1
        short_name = "_".join(words_processed) + "_{}".format(i)
    short_name_memo.add(short_name)
    return short_name


def get_wikifier_part(wikifier_input_df: pd.DataFrame):
    result = {}
    for _, each_row in wikifier_input_df.iterrows():
        result[(each_row["attribute"], each_row["context"])] = (each_row["value"])
    return result


def get_sheet_names(file_path):
    """
    This function returns the first sheet name of the excel file
    :param file_path:
    :return:
    """
    file_extension = Path(file_path).suffix
    is_csv = True if file_extension.lower() == ".csv" else False
    if is_csv:
        return [Path(file_path).name]
    xl = pd.ExcelFile(file_path)
    return xl.sheet_names


def _update_double_quotes(each_series):
    each_series["node2"] = to_kgtk_format_string(each_series["node2"])
    return each_series


def _check_double_quotes(input_df: pd.DataFrame, label_types=None, check_content_startswith: bool = False):
    output_df = input_df.copy()
    if label_types is not None:
        output_df = output_df.apply(lambda x: _update_double_quotes(x) if x["label"] in set(label_types) else x, axis=1)

    if check_content_startswith:
        output_df["node2"] = output_df['node2'].apply(
            lambda x: to_kgtk_format_string(x) if not x.startswith("Q") and not x.startswith("P") else x)
    return output_df


def _generate_p_nodes(role: str, dataset_q_node: str, node_number: int, memo: dict, node_name: str):
    """
    use memo mapping to ensure the p nodes are always correctly mapped
    :param role:
    :param dataset_q_node:
    :param node_number:
    :param memo:
    :param node_name:
    :return:
    """
    if node_name in memo['property_name_to_id']:
        p_node_id = memo['property_name_to_id'][node_name]
    else:
        p_node_id = "P{}-{}-{:03}".format(role, dataset_q_node, node_number)
        memo['property_name_to_id'][node_name] = p_node_id
    return p_node_id


def _generate_q_nodes(role: str, dataset_q_node: str, node_number: int):
    q_node_id = "Q{}-{}-{:03}".format(role, dataset_q_node, node_number)
    return q_node_id


def _generate_edge_id(node1: str, label: str, node2: str):
    if label in {"P31", "P1687", "P2006020004"}:
        id_ = "{}-{}-1".format(node1, label)
    elif label in {"label", "P1476", "description", "P1813"}:
        id_ = "{}-{}".format(node1, label)
    else:
        id_ = "{}-{}-{}".format(node1, label, node2)
    return id_


def to_kgtk_format_string(s):
    if s[0] == '"' and s[-1] == '"':
        return s
    if '"' in s:
        return "'" + s + "'"
    else:
        return '"' + s + '"'
