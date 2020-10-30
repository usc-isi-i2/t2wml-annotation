import pandas as pd
from annotation.generation.generate_t2wml import ToT2WML
from annotation.generation.generate_kgtk import GenerateKgtk
from annotation.validation.validate_annotation import ValidateAnnotation


class T2WMLAnnotation(object):
    def __init__(self):
        self.va = ValidateAnnotation()

    def process(self, dataset_qnode, df, rename_columns, extra_files=False):
        for rn in rename_columns:
            df.iloc[rn[0], rn[1]] = rn[2]

        # get the t2wml yaml file
        to_t2wml = ToT2WML(df, dataset_qnode=dataset_qnode)
        t2wml_yaml_dict = to_t2wml.get_dict()
        t2wml_yaml = to_t2wml.get_yaml()

        gk = GenerateKgtk(df, t2wml_yaml_dict, dataset_qnode=dataset_qnode, debug=True, debug_dir='/tmp')

        combined_item_def_df = pd.concat(
            [gk.output_df_dict[filename] for filename in gk.output_df_dict.keys() if filename.endswith('.tsv')])

        consolidated_wikifier_df = pd.concat([gk.constant_wikikifer_df, gk.output_df_dict["wikifier.csv"]])

        if extra_files:
            return t2wml_yaml, combined_item_def_df, consolidated_wikifier_df

        kgtk_exploded_df = gk.generate_edges_df()

        variable_ids = gk.get_variable_ids()

        return variable_ids, kgtk_exploded_df
