import pandas as pd
import tempfile
import yaml
import csv
import os
import typing
import traceback

from pathlib import Path
from t2wml.api import add_properties_from_file, KnowledgeGraph
from annotation.generation.annotation_to_template import generate_template_from_df
from annotation.generation.wikify_datamart_units_and_attributes import generate
from annotation.generation.generate_t2wml_files import execute_shell_code

# currently this script only support t2wml == 2.0a19


class GenerateKgtk:
    def __init__(self, annotated_spreadsheet: pd.DataFrame, t2wml_script: dict,
                 wikifier_file: str = None, property_file: str = None):
        """
        Parameters
        ----------
        annotated_spreadsheet: pd.DataFrame
            Annotated spreadsheet to be processed
        t2wml_script: dict
            T2WML script (from yaml)
        wikifier_file: str
            File containing general wikifier entities, such as countries
        property_file: str
            File contain general property definitions, such as the property file datamart-schema repo
        """
        self.t2wml_script = t2wml_script
        self.annotated_spreadsheet = annotated_spreadsheet
        self.property_file = property_file
        if __file__.rfind("/") != -1:
            base_pos = __file__[:__file__.rfind("/")]
        else:
            base_pos = "."
        if wikifier_file is None:
            wikifier_file = base_pos + "/country-wikifier.csv"
        if property_file is None:
            property_file = base_pos + "/datamart_schema_properties.tsv"

        self.wikifier_file = wikifier_file
        self.project_name = self.annotated_spreadsheet.iloc[0, 0]

        # generate the template files
        template_df_dict = generate_template_from_df(annotated_spreadsheet)

        # generate template output files
        self.output_df_dict = generate(template_df_dict, to_disk=False, datamart_properties_file=property_file)

        # update 2020.7.22: not add dataset edges
        _ = self.output_df_dict.pop("dataset.tsv")

        # memory all nodes2 from P1813 of variables
        variables_df = self.output_df_dict['kgtk_variables.tsv']
        self.variables_ids = variables_df[variables_df["label"] == "P1813"]["node2"].tolist()

    def get_variable_ids(self) -> typing.List[str]:
        return self.variables_ids

    def generate_edges(self, directory: str) -> str:
        """
        Returns file containing exploded KGTK edges.

        Parameters
        ----------
        directory: str
            Directory folder to store result edge file
        """
        exploded_file, metadata_file = self._make_preparations()
        # add id
        _ = exploded_file.seek(0)
        final_output_path = "{}/{}-datamart-kgtk-exploded-uniq-ids.tsv".format(directory, self.project_name)
        shell_code = """
        kgtk add_id --overwrite-id False --id-style node1-label-node2-num {} > {}
        """.format(exploded_file.name, final_output_path)
        return_res = execute_shell_code(shell_code)
        if return_res != "":
            print(return_res)
            raise ValueError("Running kgtk add-id failed! Please check!")

        # create metadata file
        shell_code = """
        kgtk explode {} --allow-lax-qnodes True --overwrite True \
        > {}/{}-datamart-kgtk-exploded_metadata.tsv
        """.format(metadata_file.name, directory, self.project_name)
        return_res = execute_shell_code(shell_code)
        if return_res != "":
            print(return_res)
            raise ValueError("Running kgtk add-id failed! Please check!")

        return final_output_path

    def generate_edges_df(self) -> pd.DataFrame:
        """
        Returns dataframe of the output from kgtk
        """
        exploded_file, metadata_file = self._make_preparations()

        # add id
        _ = exploded_file.seek(0)
        final_output_file = tempfile.NamedTemporaryFile(mode='r+', suffix=".tsv")
        final_output_path = final_output_file.name
        shell_code = """
        kgtk add_id --overwrite-id False --id-style node1-label-node2-num {} > {}
        """.format(exploded_file.name, final_output_path)
        return_res = execute_shell_code(shell_code)
        if return_res != "":
            print(return_res)
            raise ValueError("Running kgtk add-id failed! Please check!")
        _ = final_output_file.seek(0)

        final_output_df = pd.read_csv(final_output_file, sep="\t")
        return final_output_df

    def _make_preparations(self):
        """
        do the preparation steps for generate_edges and generate_edges_df
        :return:
        """
        # concat the input wikifier file with generated wikifier file from output_df_dict
        wikifier_df = pd.concat([pd.read_csv(self.wikifier_file), self.output_df_dict["wikifier.csv"]])
        temp_wikifier_file = tempfile.NamedTemporaryFile(mode='r+', suffix=".csv")
        wikifier_filepath = temp_wikifier_file.name
        wikifier_df.to_csv(wikifier_filepath, index=False)
        _ = temp_wikifier_file.seek(0)

        # use t2wml api to add properties file to t2wml database
        all_properties_df = self.output_df_dict["kgtk_properties.tsv"]
        all_properties_file = tempfile.NamedTemporaryFile(mode='r+', suffix=".tsv")
        all_properties_df.to_csv(all_properties_file.name, sep="\t", index=False)
        _ = all_properties_file.seek(0)
        add_properties_from_file(all_properties_file.name)

        # generate temp yaml file
        temp_yaml_file = tempfile.NamedTemporaryFile(mode='r+', suffix=".yaml")
        yaml_filepath = temp_yaml_file.name
        yaml.dump(self.t2wml_script, temp_yaml_file)
        temp_yaml_file.seek(0)

        # generate temp input dataset file
        temp_data_file = tempfile.NamedTemporaryFile(mode='r+', suffix=".csv")
        data_filepath = "{}.csv".format(self.project_name)

        if os.path.islink(data_filepath) or os.path.exists(data_filepath):
            os.remove(data_filepath)
        os.symlink(temp_data_file.name, data_filepath)

        self.annotated_spreadsheet.to_csv(data_filepath, header=None)
        _ = temp_data_file.seek(0)

        # generate knowledge graph
        sheet_name = data_filepath
        output_kgtk_main_content = tempfile.NamedTemporaryFile(mode='r+', suffix=".tsv")
        output_filepath = output_kgtk_main_content.name
        try:
            kg = KnowledgeGraph.generate_from_files(data_filepath, sheet_name, yaml_filepath, wikifier_filepath)
            kg.save_kgtk(output_filepath)
        except:
            traceback.print_exc()
            raise ValueError("Generating kgtk knowledge graph file failed!")
        finally:
            os.remove(data_filepath)

        _ = output_kgtk_main_content.seek(0)

        # generate imploded file
        kgtk_imploded_file = tempfile.NamedTemporaryFile(mode='r+', suffix=".tsv")
        kgtk_imploded_file_name = kgtk_imploded_file.name
        shell_code = """
            kgtk implode "{}" --remove-prefixed-columns True --without si_units language_suffix > "{}"
            """.format(output_filepath, kgtk_imploded_file_name)
        return_res = execute_shell_code(shell_code)
        if return_res != "":
            print(return_res)
            raise ValueError("Running kgtk implode failed! Please check!")
        _ = kgtk_imploded_file.seek(0)

        # concat metadata file
        metadata_df = pd.DataFrame()
        for name, each_df in self.output_df_dict.items():
            if each_df is not None and name.endswith(".tsv"):
                metadata_df = pd.concat([metadata_df, each_df])
        metadata_file = tempfile.NamedTemporaryFile(mode='r+', suffix=".tsv")
        exploded_file = tempfile.NamedTemporaryFile(mode='r+', suffix=".tsv")
        metadata_file_name = metadata_file.name
        exploded_file_name = exploded_file.name
        metadata_df.to_csv(metadata_file_name, sep="\t", index=False, quoting=csv.QUOTE_NONE)
        _ = metadata_file.seek(0)

        # combine and explode the results
        shell_code = """
        kgtk cat {} {} \
        / explode --allow-lax-qnodes True --overwrite True \
        > {}
        """.format(kgtk_imploded_file_name, metadata_file_name, exploded_file_name)
        return_res = execute_shell_code(shell_code)
        if return_res != "":
            print(return_res)
            raise ValueError("Running kgtk explode failed! Please check!")
        _ = metadata_file.seek(0)
        _ = exploded_file.seek(0)

        # validate the exploded file
        shell_code = """
        kgtk validate --allow-lax-qnodes True {}
        """.format(exploded_file_name)
        res = execute_shell_code(shell_code)
        if res != "":
            print(res)
            raise ValueError("The output kgtk file is invalid!")

        return exploded_file, metadata_file

    @staticmethod
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

