import argparse
import csv

from pathlib import Path

import pandas as pd

from annotation.generation.generate_t2wml import ToT2WML
from annotation.generation.generate_kgtk import GenerateKgtk
from annotation.validation.validate_annotation import VaidateAnnotation


def process(annotated_file: Path, output_dir: Path):
    if annotated_file.suffix not in ['.csv', '.xlsx', '.xls']:
        print(f'File type {annotated_file.suffix} not recognized.')
        print('Please upload an annotated excel file or csv file')
        return

    if annotated_file.suffix == '.csv':
        df = pd.read_csv(annotated_file, dtype=object, header=None).fillna('')
    else:
        df = pd.read_excel(annotated_file, dtype=object, header=None).fillna('')

    dataset_id = df.iloc[0,1]
    dataset_qnode = 'Q' + dataset_id

    va = VaidateAnnotation()
    validation_report, valid_annotated_file, rename_columns = va.validate(dataset_id, df=df)
    if not valid_annotated_file:
        print('Annotated file may not be valid. See output file: validation_report.json')
        with open(output_dir / 'validation_report.json', 'w') as fp:
            fp.write(validation_report)

    for rn in rename_columns:
        df.iloc[rn[0], rn[1]] = rn[2]

    to_t2wml = ToT2WML(df, dataset_qnode=dataset_qnode)
    t2wml_yaml_dict = to_t2wml.get_dict()
    t2wml_yaml = to_t2wml.get_yaml()
    open(output_dir / 't2wml.yaml', 'w').write(t2wml_yaml)

    df = df.set_index(0)

    gk = GenerateKgtk(df, t2wml_yaml_dict, dataset_qnode=dataset_qnode, debug=True, debug_dir='/tmp')
    tsv_dfs = [gk.output_df_dict[filename] for filename in gk.output_df_dict.keys() if filename.endswith('.tsv')]
    combined_tsv = pd.concat(tsv_dfs)
    combined_tsv.to_csv(output_dir / 'combined.tsv', sep='\t', index=False, quoting=csv.QUOTE_NONE)
    gk.output_df_dict['wikifier.csv'].to_csv(output_dir / 'wikifier.csv', index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process annotated file to generate t2wml yaml, wikifier and other intermediate files.')
    parser.add_argument('annotated_file', help='Path to annotated file (.csv or .xlsx)')
    parser.add_argument('--output-dir', help='Directory to place output files')
    args = parser.parse_args()
    input_file = Path(args.annotated_file).resolve()
    if args.output_dir:
        out_dir = Path(args.output_dir)
    else:
        out_dir = input_file.parent / input_file.stem
        out_dir.mkdir(exist_ok=True)

    process(input_file, out_dir)
