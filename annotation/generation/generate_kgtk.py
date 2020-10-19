import pandas as pd
import tempfile
import yaml
import csv
import os
import shutil
import typing
import traceback
from t2wml.api import KnowledgeGraph
from t2wml.wikification.utility_functions import add_entities_from_file
from annotation.generation.generate_t2wml_files import execute_shell_code
from annotation.generation.wikify_datamart_units_and_attributes import generate
from annotation.generation.annotation_to_template import generate_template_from_df, save_template_file
from time import time

# currently this script only support t2wml == 2.0a19


class GenerateKgtk:
    def __init__(self, annotated_spreadsheet: pd.DataFrame, t2wml_script: dict, dataset_qnode: str = None,
                 wikifier_file: str = None, property_file: str = None, add_datamart_constant_properties: bool = False,
                 debug: bool = False, debug_dir: str = None):
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
        self._debug = debug

        if __file__.rfind("/") != -1:
            base_pos = __file__[:__file__.rfind("/")]
        else:
            base_pos = "."
        if wikifier_file is None:
            wikifier_file = base_pos + "/country-wikifier.csv"
        if property_file is None:
            property_file = base_pos + "/datamart_schema_properties.tsv"

        self.wikifier_file = wikifier_file
        self.constant_wikikifer_df = pd.read_csv(wikifier_file)
        self.project_name = self.annotated_spreadsheet.iloc[0, 0]

        # generate the template files
        template_df_dict = generate_template_from_df(annotated_spreadsheet, dataset_qnode)

        # update 2020.7.27, enable debug to save the template and template-output files
        if self._debug:
            if debug_dir is None:
                self.debug_dir = os.path.join(os.getenv("HOME"), "datamart-annotation-debug-output")
            else:
                self.debug_dir = debug_dir
            os.makedirs(self.debug_dir, exist_ok=True)
            if not os.access(self.debug_dir, os.W_OK):
                raise ValueError("No write permission to debug folder `{}`".format(self.debug_dir))
            save_template_file(template_df_dict, os.path.join(self.debug_dir, "template.xlsx"))
            with open(os.path.join(self.debug_dir, 't2wml.yaml'), 'w') as out:
                yaml.dump(self.t2wml_script, out)
        else:
            self.debug_dir = debug_dir

        # generate template output files
        # update 2020.7.27, enable debug to save the template-output files
        self.output_df_dict = generate(loaded_file=template_df_dict,
                                       output_path=self.debug_dir,
                                       to_disk=False,
                                       datamart_properties_file=property_file,
                                       dataset_qnode=dataset_qnode,
                                       dataset_id=self.project_name,
                                       debug=self._debug,
                                       )

        # update 2020.7.22: not add dataset edges
        _ = self.output_df_dict.pop("dataset.tsv")

        # memory all nodes2 from P1813 of variables
        variables_df = self.output_df_dict['kgtk_variables.tsv']
        self.variables_ids = variables_df[variables_df["label"] == "P1813"]["node2"].tolist()

        # combine datamart-schema part's property files, we always need this for t2wml
        if property_file is None:
            property_file = __file__[:__file__.rfind("/")] + "/datamart_schema_properties.tsv"

        if not os.path.exists(property_file):
            raise ValueError("Datamart schema properties tsv file not exist at {}!".format(property_file))
        self.kgtk_properties_df = pd.concat([pd.read_csv(property_file, sep='\t'),
                                             self.output_df_dict["kgtk_properties.tsv"]])

        # update 2020.7.22: only combine datamart scheme constant properties when required
        if add_datamart_constant_properties:
            self.output_df_dict["kgtk_properties.tsv"] = self.kgtk_properties_df

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
        kgtk add_id --overwrite-id False --id-style node1-label-node2-num -i {} > {}
        """.format(exploded_file.name, final_output_path)
        s = time()
        return_res = execute_shell_code(shell_code)
        print(f'time take to run kgtk add id: {time() - s} seconds')
        if return_res != "":
            print(return_res)
            raise ValueError("Running kgtk add-id failed! Please check!")
        _ = final_output_file.seek(0)

        final_output_df = pd.read_csv(final_output_file, sep="\t", doublequote=False)

        if self._debug:
            shutil.copy(final_output_file.name, os.path.join(self.debug_dir, 'kgtk-edges.tsv'))

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
        if self._debug:
            wikifier_df.to_csv(os.path.join(self.debug_dir, "consolidated-wikifier.csv"), index=False)
        _ = temp_wikifier_file.seek(0)

        # use t2wml api to add properties file to t2wml database
        all_properties_df = self.kgtk_properties_df
        all_properties_file = tempfile.NamedTemporaryFile(mode='r+', suffix=".tsv")
        all_properties_df.to_csv(all_properties_file.name, sep="\t", index=False)
        _ = all_properties_file.seek(0)
        add_entities_from_file(all_properties_file.name)

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
        t2wml_output_filepath = output_kgtk_main_content.name
        try:
            s = time()
            kg = KnowledgeGraph.generate_from_files(data_filepath, sheet_name, yaml_filepath, wikifier_filepath)
            kg.save_kgtk(t2wml_output_filepath)
            print(f'time take to get t2wml output: {time() - s} seconds')
        except:
            traceback.print_exc()
            raise ValueError("Generating kgtk knowledge graph file failed!")
        finally:
            os.remove(data_filepath)

        t2wml_kgtk_df = pd.read_csv(t2wml_output_filepath, sep="\t", doublequote=False)
        if len(t2wml_kgtk_df) == 0:
            raise ValueError("An empty kgtk file was generated from t2wml! Please check!")

        _ = output_kgtk_main_content.seek(0)

        # generate imploded file
        kgtk_imploded_file = tempfile.NamedTemporaryFile(mode='r+', suffix=".tsv")
        kgtk_imploded_file_name = kgtk_imploded_file.name
        shell_code = """
            kgtk implode -i "{}" --allow-lax-qnodes --remove-prefixed-columns True --without si_units language_suffix > "{}"
            """.format(t2wml_output_filepath, kgtk_imploded_file_name)
        s = time()
        return_res = execute_shell_code(shell_code)
        print(f'time take to run kgtk implode: {time() - s} seconds')
        if return_res != "":
            print(return_res)
            raise ValueError("Running kgtk implode failed! Please check!")
        _ = kgtk_imploded_file.seek(0)

        # concat metadata file
        metadata_df = pd.DataFrame()
        for name, each_df in self.output_df_dict.items():
            if each_df is not None and name.endswith(".tsv"):
                if name.strip() != 'datamart_schema_properties.tsv':
                    metadata_df = pd.concat([metadata_df, each_df])
        metadata_file = tempfile.NamedTemporaryFile(mode='r+', suffix=".tsv")
        exploded_file = tempfile.NamedTemporaryFile(mode='r+', suffix=".tsv")
        metadata_file_name = metadata_file.name
        exploded_file_name = exploded_file.name
        metadata_df.to_csv(metadata_file_name, sep="\t", index=False, quoting=csv.QUOTE_NONE)
        _ = metadata_file.seek(0)

        # combine and explode the results
        shell_code = """
        kgtk cat -i {} {} \
        / explode --allow-lax-qnodes True --overwrite True \
        > {}
        """.format(kgtk_imploded_file_name, metadata_file_name, exploded_file_name)
        s = time()
        return_res = execute_shell_code(shell_code)
        print(f'time take to run kgtk cat and explode: {time() - s} seconds')
        if return_res != "":
            print(return_res)
            raise ValueError("Running kgtk explode failed! Please check!")
        # _ = metadata_file.seek(0)
        # _ = exploded_file.seek(0)

        # validate the exploded file
        # shell_code = """
        # kgtk validate --allow-lax-qnodes True {}
        # """.format(exploded_file_name)
        # s = time()
        # res = execute_shell_code(shell_code)
        # print(f'time take to run kgtk validate: {time() - s} seconds')
        # if res != "":
        #     print(res)
        #     raise ValueError("The output kgtk file is invalid!")

        return exploded_file, metadata_file
