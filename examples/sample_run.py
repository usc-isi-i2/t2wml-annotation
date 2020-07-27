import yaml
import pandas as pd
from annotation.generation.generate_kgtk import GenerateKgtk

def main():
    """
    example: use the 2 sample file in current folder
    TODO: currently ethiopia wikifier need vpn access, we should put it to service and not need vpn later
    After running finish, you will see 2 output file at current folder:
    awsd-datamart-kgtk-exploded_metadata.tsv
    awsd-datamart-kgtk-exploded-uniq-ids.tsv
    """
    input_path = "./sample_annotation_file.xlsx"
    yaml_path = "./sample_yaml_file.yaml"
    input_df = pd.read_excel(input_path, index_col=0, header=None)
    with open(yaml_path, "r") as f:
        t2wml_script = yaml.load(f, Loader=yaml.FullLoader)
    test = GenerateKgtk(annotated_spreadsheet=input_df, t2wml_script=t2wml_script,
                        dataset_qnode="QTEST01", add_datamart_constant_properties=False, debug=True)
    # you can also send parameter as debug_dir="path_to_folder" to specify the output debugging file,
    # default it will save to user's home directory + "datamart-annotation-debug-output"

    # output to current directory
    test.generate_edges("./")

    # or output to dataframe
    output = test.generate_edges_df()
    print("------------output kgtk file is------------")
    print(output)

    # get all variable ids
    print("-------------all variable ids--------------")
    print(test.get_variable_ids())


if __name__ == "__main__":
    main()
