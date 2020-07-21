import pandas as pd


class GenerateKgtk:
    def __init__(self, annotated_spreadsheet: pd.Dataframe, t2wml_script: dict,
                 wikifier_file: str, property_file: str):
        """
        Parameters
        ----------
        annotated_spreadsheet: pd.Dataframe
            Annotated spreadsheet to be processed
        t2wml_script: dict
            T2WML script (from yaml)
        wikifier_file: str
            File containing general wikifier entities, such as countries
        property_file: str
            File contain general property definitions, such as the property file datamart-schema repo
        """

        pass

    def generate_edges(self, directory: str) -> str:
        '''
        Returns file containing exploded KGTK edges.

        Parameters
        ----------
        directory: str
            Directory folder to store result edge file
        '''
        pass
