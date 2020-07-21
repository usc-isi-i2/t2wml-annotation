import pandas as pd
import os
import logging

_logger = logging.getLogger(__name__)
type_mapper_dict = {"string": "String", "number": "Quantity", "year": "Time", "month": "Time", "day": "Time"}


def generate_dataset_tab(dataset_id: str) -> pd.DataFrame:
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


def generate_template_from_df(input_df: pd.DataFrame) -> dict:
    """
    function used for datamart annotation batch mode, return a dict of dataFrame instead of output a xlsx file
    :param input_df: input annotation DataFrame
    :return:
    """
    dataset_id = input_df.iloc[0, 0]
    annotation_part = input_df.iloc[1:7].fillna("")
    content_part = input_df.iloc[7:]
    # fix condition is we have merged roles rows
    for i in range(1, annotation_part.shape[1]):
        if annotation_part.iloc[:, i]["role"] == "":
            annotation_part.iloc[:, i]["role"] = annotation_part.iloc[:, i - 1]["role"]

    # start generate dataframe for templates
    dataset_df = generate_dataset_tab(dataset_id)
    attribute_df = generate_attributes_tab(dataset_id, annotation_part)
    unit_df = generate_unit_tab(dataset_id, content_part, annotation_part)
    extra_df, wikifier_df1 = process_main_subject(dataset_id, content_part, annotation_part)
    wikifier_df2 = generate_wikifier_part(content_part, annotation_part)
    wikifier_df = pd.concat([wikifier_df1, wikifier_df2])

    output_df_dict = {
        'dataset_file': dataset_df,
        'attributes_file': attribute_df,
        'units_file': unit_df,
        "Extra extra_edges": extra_df,
        "Wikifier_t2wml": wikifier_df,
        "wikifier": None,
        "qualifiers": None,
        "extra_edges": None,
    }
    return output_df_dict


def generate_template(input_path: str, output_path: str) -> None:
    """
    genearte the template xlsx file from the input xlsx file
    :param input_path:
    :param output_path:
    :return:
    """
    input_df = pd.read_excel(input_path, index_col=0, header=None)
    dataset_id = input_df.iloc[0, 0]
    annotation_part = input_df.iloc[1:7].fillna("")
    content_part = input_df.iloc[7:]

    # fix condition is we have merged roles rows, it may cause wrong things
    for i in range(1, annotation_part.shape[1]):
        if annotation_part.iloc[:, i]["role"] == "":
            previous_role = annotation_part.iloc[:, i - 1]["role"]
            _logger.warning("No role detect on column No.{}, will assume from previous column as {}"
                            .format(i, annotation_part.iloc[:, i - 1]["role"]))
            annotation_part.iloc[:, i]["role"] = previous_role

    # start generate dataframe for templates
    dataset_df = generate_dataset_tab(dataset_id)
    attribute_df = generate_attributes_tab(dataset_id, annotation_part)
    unit_df = generate_unit_tab(dataset_id, content_part, annotation_part)
    extra_df, wikifier_df1 = process_main_subject(dataset_id, content_part, annotation_part)
    wikifier_df2 = generate_wikifier_part(content_part, annotation_part)
    wikifier_df = pd.concat([wikifier_df1, wikifier_df2])

    with pd.ExcelWriter(output_path) as writer:
        dataset_df.to_excel(writer, sheet_name='Dataset', index=False)
        attribute_df.to_excel(writer, sheet_name='Attributes', index=False)
        unit_df.to_excel(writer, sheet_name='Units', index=False)
        extra_df.to_excel(writer, sheet_name="Extra Edges", index=False)
        wikifier_df.to_excel(writer, sheet_name="Wikifier_t2wml", index=False)


def generate_attributes_tab(dataset_id: str, annotation_part: pd.DataFrame) -> pd.DataFrame:
    """
        codes used to generate the template attribute tab
        1. add for columns with role = variable or role = qualifier. 
    """
    attributes_df_list = []

    for i in range(annotation_part.shape[1]):
        each_col_info = annotation_part.iloc[:, i]
        role = each_col_info["role"].lower()
        if role in {"variable", "qualifier"}:
            attribute = each_col_info["header"]
            role_type = each_col_info["type"].lower()
            if role_type not in type_mapper_dict:
                raise ValueError("Column type {} for column {} is not valid!".format(role_type, i))
            data_type = type_mapper_dict[each_col_info["type"]]
            label = "{}".format(attribute) if not each_col_info['name'] else each_col_info['name']
            description = "{} column in {}".format(role, dataset_id) if not each_col_info['description'] \
                else each_col_info['description']
            attributes_df_list.append(
                {"Attribute": attribute, "Property": "", "type": data_type, "label": label, "description": description})

    if len(attributes_df_list) == 0:
        attributes_df = pd.DataFrame(columns=['Attribute', 'Property', 'label', 'description'])
    else:
        attributes_df = pd.DataFrame(attributes_df_list)
    return attributes_df


def generate_unit_tab(dataset_id: str, content_part: pd.DataFrame, annotation_part: pd.DataFrame) -> pd.DataFrame:
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
    unit_cols = []
    units_set = set()

    for i in range(annotation_part.shape[1]):
        each_col_info = annotation_part.iloc[:, i]
        role = each_col_info["role"].lower()
        # if role is unit, record them
        if role == "unit":
            unit_cols.append(i)
        # add units defined in unit
        if each_col_info['unit'] != "":
            units_set.add(each_col_info['unit'].lower())

    if len(unit_cols) > 0:
        units_set.update(content_part.iloc[:, unit_cols].agg(", ".join, axis=1).unique())

    # sort for better index for human vision
    for each_unit in sorted(list(units_set)):
        unit_df_list.append({"Unit": each_unit, "Q-Node": ""})

    if len(unit_df_list) == 0:
        unit_df = pd.DataFrame(columns=['Unit', 'Q-Node'])
    else:
        unit_df = pd.DataFrame(unit_df_list)

    return unit_df


def process_main_subject(dataset_id: str, content_part: pd.DataFrame, annotation_part: pd.DataFrame):
    row_offset = 7
    col_offset = 1
    wikifier_df_list = []
    extra_df_list = []
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
                    node = "{}-{}".format(dataset_id, label)
                    wikifier_df_list.append(
                        {"column": i + col_offset, "row": row + row_offset, "value": label, "context": "main subject",
                         "item": node})
                    labels = ["label", "description", "P31"]
                    node2s = ["{} {}".format(main_subject_annotation["header"], label),
                              main_subject_annotation["description"], "Q35120"]
                    for each_label, each_node2 in zip(labels, node2s):
                        extra_df_list.append({"id": "", "node1": node, "label": each_label, "node2": each_node2})

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


def generate_wikifier_part(content_part: pd.DataFrame, annotation_part: pd.DataFrame):
    # generate wikifier file for all columns that have type == country, admin1, admin2, or admin3
    # TODO: set country wikifier and ethiopia wikifier to be a service
    wikifier_df_list = []
    target_cols = []
    run_ethiopia_wikifier = False
    row_offset = 7
    col_offset = 1
    for i in range(annotation_part.shape[1]):
        each_col_info = annotation_part.iloc[:, i]
        if each_col_info["role"] == "location":
            if each_col_info["type"] == "country":
                # use country wikifier
                from annotation.generation.country_wikifier import DatamartCountryWikifier
                wikified_result = DatamartCountryWikifier().wikify(content_part.iloc[:, i].dropna().unique().tolist())
                for label, node in wikified_result.items():
                    wikifier_df_list.append(
                        {"column": "", "row": "", "value": label, "context": "", "item": node})
                if "ethiopia" in [each.lower() for each in content_part.iloc[:, i].dropna().unique()]:
                    run_ethiopia_wikifier = True
            if each_col_info["type"] in {"admin1", "admin2", "admin3"}:
                target_cols.append(i)

    if run_ethiopia_wikifier:
        from annotation.generation.ethiopia_wikifier import EthiopiaWikifier
        wikifier = EthiopiaWikifier()
        # get target columns to run with wikifier
        target_df = content_part.iloc[:, target_cols].reset_index().drop(columns=[0])
        # run wikifier on each column
        for i in range(len(target_cols)):
            # for each part, run wikifier and add it the wikifier file
            input_col_name = target_df.columns[i]
            output_col_name = "{}_wikifier".format(input_col_name)
            wikifier_res = wikifier.produce(input_df=target_df, target_column=input_col_name).fillna("")
            for row_number, each_row in wikifier_res.iterrows():
                label = each_row[input_col_name]
                node = each_row[output_col_name]
                if node != "" and label != "":
                    # to prevent duplicate names with different nodes, we need to create column and row number here
                    wikifier_df_list.append(
                        {"column": target_cols[i] + col_offset, "row": row_number + row_offset, "value": label,
                         "context": "", "item": node})
    if len(wikifier_df_list) == 0:
        wikifier_df = pd.DataFrame(columns=['column', 'row', 'value', 'context', "item"])
    else:
        wikifier_df = pd.DataFrame(wikifier_df_list)
    return wikifier_df

