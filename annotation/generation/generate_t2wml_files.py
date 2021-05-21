import os
from pathlib import Path
from subprocess import Popen, PIPE
import pandas as pd
import shutil

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


def produce(t2wml_project_path: str, project_name: str, input_folder_path:str, output_folder_path: str):
    # set up the environment
    virtual_env = pd.__file__.replace("pandas/__init__.py", "backend")
    os.chdir(virtual_env)
    from driver import run_t2wml

    # set up the folders
    yaml_file = os.path.join(t2wml_project_path, "{}/{}.yaml".format(project_name, project_name))
    wikifier_file = os.path.join(output_folder_path, "consolidated-wikifier.csv")
    data_file_folder = input_folder_path
    output_directory = os.path.join(output_folder_path, "t2wml-output")

    for filename in os.listdir(data_file_folder):
        if filename.endswith(".csv"):
            total_results = ""
            data_file_path = os.path.join(data_file_folder, filename)
            print("processing", filename)
            sheet_names = get_sheet_names(data_file_path)
            for sheet_name in sheet_names:
                run_t2wml(data_file_path, wikifier_file, yaml_file, output_directory, sheet_name, filetype="tsv",
                          project_name=project_name)


    # move all files from folder
    for each_file in os.listdir(output_directory):
        full_path = os.path.join(output_directory, each_file)
        if os.path.isdir(full_path):
            file_path = os.path.join(output_directory, each_file, "results.tsv")
            if os.path.isfile(file_path):
                shutil.move(file_path, full_path + ".tsv")
            shutil.rmtree(full_path)


def execute_shell_code(shell_command: str, debug=True):
    if debug:
        print("Executing...")
        print(shell_command)
        print("-" * 100)
    out = Popen(shell_command, shell=True, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    # out.wait()
    """
    Popen.wait():

    Wait for child process to terminate. Set and return returncode attribute.

    Warning: This will deadlock when using stdout=PIPE and/or stderr=PIPE and the child process generates enough output to
    a pipe such that it blocks waiting for the OS pipe buffer to accept more data. Use communicate() to avoid that. """
    stdout, stderr = out.communicate()
    if stderr:
        print("Error!!")
        print(stderr)
        print("-" * 50)
        raise

    if debug:
        print("Running finished!!!!!!")
    return stdout


# Not used?
# def populate_node2_columns(folder_path: str):
#     t2wml_output_path = os.path.join(folder_path, "t2wml-output")
#     output_path = None
#     if not os.path.exists(os.path.join(folder_path, "imploded")):
#         os.mkdir(os.path.join(folder_path, "imploded"))
#     for each_file in os.listdir(t2wml_output_path):
#         full_path = os.path.join(t2wml_output_path, each_file)
#         if os.path.isfile(full_path) and each_file.endswith(".tsv"):
#             # print("processing", full_path)
#             output_path = os.path.join(folder_path, "imploded", each_file)
#             shell_code = """
#             kgtk implode -i "{}" --remove-prefixed-columns True --without si_units language_suffix -o "{}"
#             """.format(full_path, output_path)
#             execute_shell_code(shell_code)
#     if not output_path:
#         raise ValueError("No tsv file found to populate!")
#     return output_path
