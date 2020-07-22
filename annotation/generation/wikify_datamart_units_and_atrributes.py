import pandas as pd
import os
import string
import csv
import copy
import typing

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
    if not sheet_name_config:
        sheet_name_config = {"dataset_file": "Dataset",
                             "attributes_file": "Attributes",
                             "units_file": "Units",
                             "extra_edges": "Extra Edges"
                             }
    for k, v in sheet_name_config.items():
        loaded_file[k] = pd.read_excel(input_file, v)
    try:
        loaded_file["wikifier"] = pd.read_excel(input_file, "Wikifier")
    except:
        loaded_file["wikifier"] = None
    # added 2020.6.30, for Qualifier
    try:
        loaded_file["qualifiers"] = pd.read_excel(input_file, "Qualifiers")
    except:
        loaded_file["qualifiers"] = None
    # added 2020.7.13, for t2wml format wikifier input
    try:
        loaded_file["Wikifier_t2wml"] = pd.read_excel(input_file, "Wikifier_t2wml")
    except:
        loaded_file["Wikifier_t2wml"] = None
    return loaded_file


def generate(loaded_file: dict, output_path: str = ".", column_name_config=None, to_disk=True,
             datamart_properties_file: str = None) -> typing.Optional[dict]:
    """
    The main entry function for generating datamart files from template input,
    base on input parameter `to_disk`, the output can be None or dict of dataframe
    :param loaded_file:
    :param output_path:
    :param column_name_config:
    :param to_disk:
    :param datamart_properties_file
    :return:
    """
    if not os.path.exists(output_path):
        os.mkdir(output_path)

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

    dataset_id = loaded_file["dataset_file"]["dataset"].iloc[0]
    # generate files
    memo = defaultdict(dict)
    kgtk_properties_df = generate_KGTK_properties_file(loaded_file["attributes_file"], loaded_file["qualifiers"],
                                                       dataset_id,
                                                       memo, column_name_config["attributes_file_node_column_name"],
                                                       column_name_config["attributes_file_node_label_column_name"])

    kgtk_variables_df = generate_KGTK_variables_file(loaded_file["attributes_file"], dataset_id, memo,
                                                     column_name_config["attributes_file_node_column_name"],
                                                     column_name_config["attributes_file_node_label_column_name"])

    kgtk_units_df = generate_KGTK_units_file(loaded_file["units_file"], dataset_id, memo,
                                             column_name_config["unit_file_node_column_name"],
                                             column_name_config["unit_file_node_label_column_name"])

    wikifier_df = generate_wikifier_file(memo, extra_wikifier_dict)
    if loaded_file["Wikifier_t2wml"] is not None:
        wikifier_df = pd.concat([wikifier_df, loaded_file["Wikifier_t2wml"]])

    dataset_df = generate_and_save_dataset_file(loaded_file["dataset_file"])
    extra_edges_df = generate_extra_edges_file(loaded_file["extra_edges"], memo)

    # combine datamart-schema part's property files
    if datamart_properties_file is None:
        datamart_properties_file = __file__[:__file__.rfind("/")] + "/datamart_schema_properties.tsv"

    if not os.path.exists(datamart_properties_file):
        raise ValueError("Datamart schema properties tsv file not exist at {}!".format(datamart_properties_file))
    kgtk_properties_df = pd.concat([pd.read_csv(datamart_properties_file, sep='\t'), kgtk_properties_df])

    output_files = [kgtk_properties_df, kgtk_variables_df, kgtk_units_df, wikifier_df, extra_edges_df, dataset_df]
    output_file_names = ["kgtk_properties.tsv", "kgtk_variables.tsv", "kgtk_units.tsv", "wikifier.csv", "extra_edges.tsv", "dataset.tsv"]
    if not to_disk:
        result_dict = {}
        for each_file, each_file_name in zip(output_files, output_file_names):
            result_dict[each_file_name] = each_file
        return result_dict

    else:
        if not os.path.exists(output_path):
            os.mkdir(output_path)
        for each_file, each_file_name in zip(output_files, output_file_names):
            output_file_path = os.path.join(output_path, each_file_name)
            if each_file_name.endswith(".csv"):
                each_file.to_csv(output_file_path, index=False)
            elif each_file_name.endswith(".tsv"):
                each_file.to_csv(output_file_path, sep='\t', index=False, quoting=csv.QUOTE_NONE)


def generate_KGTK_properties_file(input_df: pd.DataFrame, qualifier_df: pd.DataFrame, dataset_id: str, memo: dict,
                                  node_column_name="Property", node_label_column_name="Attribute",
                                  qualifier_column_name="Qualifiers") -> pd.DataFrame:
    """
    sample format for each property (totally 3 rows)
    Please note that data type may change (to String, Date) base on the given input template file
        id	                            node1	            label	    node2
	0   Paid-security-002-data_type	    Paid-security-002	data_type	Quantity
    1   Paid-security-002-P31	        Paid-security-002	P31	        Q18616576
    2   Paid-security-002-label	        Paid-security-002	label	    UN

    :return:
    """
    node_number = 1
    output_df_list = []
    input_df = input_df.fillna("")
    for _, each_row in input_df.iterrows():
        node_number += 1
        if each_row[node_column_name] == "":
            node_label = to_kgtk_format_string(each_row[node_label_column_name])
            node_id = "P{}-{:03}".format(dataset_id, node_number)

            # add to memo for future use
            memo["property"][node_id] = each_row[node_label_column_name]
            memo["property_role"][node_id] = each_row["Role"].lower()
            memo["property_name_to_id"][each_row[node_label_column_name]] = node_id

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
            memo["property"][each_row[node_column_name]] = each_row[node_label_column_name]

    # add qualifier part if we have
    if qualifier_df is not None:
        qualifier_df = qualifier_df.fillna("")
        for _, each_row in qualifier_df.iterrows():
            node_number += 1
            if each_row[node_column_name] == "":
                node_id = "P{}-{:03}".format(dataset_id, node_number)
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


def generate_KGTK_variables_file(input_df: pd.DataFrame, dataset_id: str, memo: dict, node_column_name="Property",
                                 node_label_column_name="Attribute"):
    """
    sample format for each variable (totally 9 rows)
        "id"                        "node1"    "label"          "node2"
    0   QOECD-002-label             QOECD-002   label           "GDP per capita"
    1   QOECD-002-P1476             QOECD-002   P1476           "GDP per capita"
    2   QOECD-002-description       QOECD-002   description     "GDP per capita variable in OECD"
    3   QOECD-002-P31-1             QOECD-002   P31             Q50701
    4   QOECD-002-P1687-1           QOECD-002   P1687           POECD-002
    5   QOECD-002-P2006020002-P248  QOECD-002   P2006020002     P248
    6   QOECD-002-P2006020004-1     QOECD-002   P2006020004     QOECD
    7   QOECD-002-P1813             QOECD-002   P1813           "gdp_per_capita"
    8   QOECD-P2006020003-QOECD002  QOECD       P2006020003     QOECD-002

    following part length will change depending on the properties amount



    """
    node_number = 1
    output_df_list = []
    short_name_memo = set()
    input_df = input_df.fillna("")

    all_qualifier_properties = []
    for node, role in memo["property_role"].items():
        if role == "qualifier":
            all_qualifier_properties.append(node)

    for _, each_row in input_df.iterrows():
        relations = each_row['Relationship']
        target_properties = []

        # qualifier should not have qualifier properties
        if each_row['Role'].lower() != "qualifier":
            if relations == "":
                target_properties = all_qualifier_properties
            else:
                for each_relation in relations.slipt("|"):
                    if each_relation not in memo["property_name_to_id"]:
                        raise ValueError("Annotation specify variable {} not exist in input data.".format(each_relation))
                    target_properties.append(memo["property_name_to_id"][each_relation])

        node_number += 1
        if each_row[node_column_name] == "":
            p_node_id = "P{}-{:03}".format(dataset_id, node_number)
        else:
            p_node_id = each_row[node_column_name]
        q_node_id = "Q{}-{:03}".format(dataset_id, node_number)
        memo["variable"][q_node_id] = each_row[node_label_column_name]

        labels = ["label", "P1476", "description",
                  "P31", "P1687",
                  "P2006020002", "P2006020004", "P1813",
                  "P2006020003"] + len(target_properties) * ["P2006020002"]
        node2s = [to_kgtk_format_string(each_row[node_label_column_name]),  # 1
                  to_kgtk_format_string(each_row[node_label_column_name]),  # 2
                  to_kgtk_format_string("{} in {}".format(each_row[node_label_column_name], dataset_id)),  # 3
                  "Q50701", p_node_id,  # 4-5
                  "P248",  # 6
                  "Q" + dataset_id,  # 7
                  get_short_name(short_name_memo, each_row[node_label_column_name]),  # 8
                  q_node_id  # 9
                  ] + target_properties
        node1s = [q_node_id] * 9 + ["Q" + dataset_id] + [q_node_id] * len(target_properties)

        # add those nodes
        for i, each_label in enumerate(labels):
            if each_label in {"P31", "P1687", "P2006020004"}:
                id_ = "{}-{}-1".format(node1s[i], labels[i])
            elif each_label in {"label", "P1476", "description",  "P1813"}:
                id_ = "{}-{}".format(node1s[i], labels[i])
            else:
                id_ = "{}-{}-{}".format(node1s[i], labels[i], node2s[i])
            output_df_list.append({"id": id_, "node1": node1s[i], "label": labels[i], "node2": node2s[i]})

    # get output
    output_df = pd.DataFrame(output_df_list)
    # in case of empty df
    if output_df.shape == (0, 0):
        output_df = pd.DataFrame(columns=['id', 'node1', 'label', 'node2'])
    return output_df


def generate_KGTK_units_file(input_df: pd.DataFrame, dataset_id: str, memo: dict, node_column_name="Q-Node",
                             node_label_column_name="Unit") -> pd.DataFrame:
    """
        sample format for each unit (totally 2 rows)
        id	                        node1	            label	node2
    0   Qaid-security-U002-label	Qaid-security-U002	label	person
    1   Qaid-security-U002-P31	    Qaid-security-U002	P31	    Q47574

    :return:
    """
    node_number = 1
    count = 0
    output_df_dict = {}
    input_df = input_df.fillna("")
    for _, each_row in input_df.iterrows():
        node_number += 1
        if each_row[node_column_name] == "":
            node_id = "Q{}-U{:03}".format(dataset_id, node_number)
            labels = ["label", "P31"]
            node2s = [to_kgtk_format_string(each_row[node_label_column_name]), "Q47574"]
            memo["unit"][node_id] = each_row[node_label_column_name]
            for i in range(2):
                id_ = "{}-{}".format(node_id, labels[i])
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


def generate_wikifier_file(memo, extra_wikifier_dict):
    """
    generate the wikifier part from template(those properties, variables, units generated in above functions)
    Sample file looks like:
        column	row	value	    context	    item
	0	            UN	        property	Paid-security-002
	1	            INGO	    property	Paid-security-003
	2	            LNGO/NRCS	property	Paid-security-004
	3	            ICRC	    property	Paid-security-005
	4		        UN	        variable	Qaid-security-002
	5	            INGO	    variable	Qaid-security-003
    6               person	    unit	    Qaid-security-U002
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


def generate_and_save_dataset_file(input_df: pd.DataFrame):
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
    output_df = check_double_quotes(output_df, check_content_startswith=True)
    return output_df


def generate_extra_edges_file(input_df: pd.DataFrame, memo: dict):
    qualifier_extra_edges_list = []
    if "qualifier_target_nodes" in memo:
        for k, v in memo['qualifier_target_nodes'].items():
            qualifier_extra_edges_list.append({"id": "", "node1": v, "label": "P2006020002",
                                               "node2": memo["qualifier_name_to_id"][k]})

    output_df = pd.concat([input_df, pd.DataFrame(qualifier_extra_edges_list)])
    # check double quotes
    output_df = check_double_quotes(output_df, label_types={"label", "description"})
    return output_df


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


def _update_double_quotes(each_series):
    each_series["node2"] = to_kgtk_format_string(each_series["node2"])
    return each_series


def check_double_quotes(input_df: pd.DataFrame, label_types=None, check_content_startswith: bool = False):
    output_df = input_df.copy()
    if label_types is not None:
        output_df = output_df.apply(lambda x: _update_double_quotes(x) if x["label"] in set(label_types) else x, axis=1)

    if check_content_startswith:
        output_df["node2"] = output_df['node2'].apply(
            lambda x: to_kgtk_format_string(x) if not x.startswith("Q") and not x.startswith("P") else x)
    return output_df


def to_kgtk_format_string(s):
    if s[0] == '"' and s[-1] == '"':
        return s
    if '"' in s:
        return "'" + s + "'"
    else:
        return '"' + s + '"'
