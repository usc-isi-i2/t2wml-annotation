import csv
import os
import pandas as pd
import numpy as np
import typing
import string
import tempfile
import re
import urllib.parse

from annotation.generation.country_wikifier import HybridJaccardSimilarity
from collections import defaultdict
from io import StringIO
from tl.utility.utility import Utility

ethiopia_direction_dict = {"misraq": "east", "misraqawi": "eastern",
                           "mirab": "west", "mi'irabawi": "western",
                           "debub": "south", "debubawi": "southern",
                           "semien": "north",
                           }
CONSTRAINS_CHARS = set("abcdefghijklmnopqrstuvwxyz_() 1234567890'")

ethiopia_census_code = {}
with open(os.path.join(os.path.dirname(__file__), 'ethiopia_census_code.csv'), 'r') as fin:
    reader = csv.reader(fin)
    for row in reader:
        ethiopia_census_code[row[0]] = row[1]

class EthiopiaWikifier:
    def __init__(self, es_server=None, es_index=None, sparql_server=None, similarity_threshold: float = 0.5):
        if not es_server:
            # self.es_server = "http://kg2018a.isi.edu:9200"
            self.es_server = "https://dsbox02.isi.edu:8888/es"
        else:
            self.es_server = es_server
        if not es_index:
            self.es_index = "ethiopia_wikifier_index"
        else:
            self.es_index = es_index
        if not sparql_server:
            self.sparql_server = "https://dsbox02.isi.edu:8888/bigdata/namespace/wdq/sparql"
        else:
            self.sparql_server = sparql_server
        self.level_memo = defaultdict(int)
        self.TRANSLATOR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))
        self.similarity_unit = HybridJaccardSimilarity(tl_args={"ignore_case": True}, tokenizer="word")
        self.similarity_threshold = similarity_threshold
        self.admin_level_mapping = {"admin1": 1, "admin2": 2, "admin3": 3}
        self.level_restrict = None

    def generate_index(self, kgtk_file: str, output_path: str):
        """
        Generate the updated kgtk index to upload the elastic search
        """
        label_memo = defaultdict(set)
        with open(kgtk_file, "r") as f:
            _ = f.readline()
            for each_line in f.readlines():
                each_line = each_line.replace("\n", "")
                if each_line.endswith("\t"):
                    each_line = each_line[:-1]
                node1, label, node2 = each_line.split("\t")
                if node2[0] == '"' and node2[-1] == '"':
                    node2 = node2[1:-1]
                if label == "label":
                    label_memo[node1].add(node2)
                    for each in ["(woreda)", "Zone", "Region", "District"]:
                        if each in node2:
                            label_memo[node1].add(node2.replace(each, "").strip())
                    if "," in node2:
                        label_memo[node1].add(node2.split(",")[0])

        label_memo = self.get_extra_names(label_memo)

        new_label_memo = {}

        for node, labels in label_memo.items():
            new_labels = set()
            for each_label in labels:
                temp = each_label.lower().strip()
                new_labels.add(temp)
                for v1, v2 in ethiopia_direction_dict.items():
                    if v1 in temp:
                        new_labels.add(temp.replace(v1, v2))
            new_label_memo[node] = new_labels

        label_memo = new_label_memo

        with open(kgtk_file, "r") as f:
            output_f = open(output_path, "w")
            _ = output_f.write(f.readline())
            names = [[], [], []]
            current_node1 = None
            current_node1_label = None
            for each_line in f.readlines():
                each_line_origin = each_line
                each_line = each_line.replace("\n", "")
                if each_line.endswith("\t"):
                    each_line = each_line[:-1]
                node1, label, node2 = each_line.split("\t")
                if node2[0] == '"' and node2[-1] == '"':
                    node2 = node2[1:-1]
                if node1 != current_node1:
                    if current_node1 is not None:
                        temp = label_memo[current_node1] - {current_node1_label}
                        if len(temp) > 0:
                            for each in temp:
                                res = [current_node1, "label", each]
                                _ = output_f.write("\t".join(res) + "\n")
                        for each in self.get_permunations(names, [[]], 0):
                            res = [current_node1, "aliases", "<".join(each[::-1])]
                            _ = output_f.write("\t".join(res) + "\n")
                        current_node1 = node1
                        names = [[], [], []]
                    else:
                        current_node1 = node1
                if label == "label":
                    current_node1_label = node2
                if label in {"P2006190001", "P2006190002", "P2006190003"}:
                    names[int(label[-1]) - 1] = label_memo[node2]
                _ = output_f.write(each_line_origin)
            output_f.close()

    def combine_ifexists(self, label_memo, ifexists_file: str):
        """
        used to combine the other aliases / information from kgtk wikidata dump to current file
        :param label_memo:
        :param ifexists_file:
        :return:
        """
        df = pd.read_csv(ifexists_file, sep="\t")
        add_count = 0
        for _, each_row in df.iterrows():
            if each_row["label"] in {"label", "aliases"}:
                node1 = each_row["node1"]
                node2 = each_row['node2']
                if isinstance(node2, str):
                    node2 = node2.replace("\\'", "")
                    if "@en" in node2:
                        node2 = node2[:-3]
                    if node2[0] == "'" and node2[-1] == "'":
                        node2 = node2[1:-1]
                    if " (woreda)" in node2:
                        node2 = node2.replace(" (woreda)", "")
                    if node2 not in label_memo[node1]:
                        add_count += 1
                        label_memo[node1].add(node2)

    def get_permunations(self, names, results, i):
        if i == len(names):
            return results
        if len(names[i]) > 0:
            new_results = []
            for each_result in results:
                for each in names[i]:
                    new_results.append(each_result + [each])
        else:
            new_results = results
        return self.get_permunations(names, new_results, i + 1)

    @staticmethod
    def send_sparql_query(query_body: str, query_address: str):
        """
            a simple wrap to send the query and return the returned results
        """
        from SPARQLWrapper import SPARQLWrapper, JSON, POST, URLENCODED  # type: ignore
        qm = SPARQLWrapper(query_address)
        qm.setReturnFormat(JSON)
        qm.setMethod(POST)
        qm.setRequestMethod(URLENCODED)
        qm.setQuery(query_body)
        try:
            results = qm.query().convert()['results']['bindings']
            return results
        except Exception as e:
            error_message = ("Sending Sparql query to {} failed!".format(query_address))
            raise ValueError(error_message)

    def upload_to_es(self, kgtk_file: str):
        """
            main function call to upload the index
        """

        output_json = tempfile.NamedTemporaryFile(mode='r+')
        map_json = tempfile.NamedTemporaryFile(mode='r+')
        kgtk_index = tempfile.NamedTemporaryFile(mode='r+')
        self.generate_index(kgtk_file, kgtk_index.name)
        _ = kgtk_index.seek(0)
        # build index
        Utility.build_elasticsearch_file(kgtk_index.name, "preflabel,label", map_json.name, output_json.name, "aliases")
        # upload
        _ = map_json.seek(0)
        _ = output_json.seek(0)
        Utility.load_elasticsearch_index(output_json.name, self.es_server, self.es_index, map_json.name)

    def get_extra_names(self, label_memo):
        target_nodes = label_memo.keys()
        each_part_str = " ".join(["wd:{}".format(each) for each in target_nodes])
        query = """
        SELECT DISTINCT ?item ?article WHERE {{
          values ?item {{{q_nodes}}}
          ?article schema:about ?item ;
          FILTER (SUBSTR(str(?article), 12, 13) = "wikipedia.org")
        }}
        """.format(q_nodes=each_part_str)

        result = self.send_sparql_query(query, self.sparql_server)
        for each in result:
            node = each['item']['value'].split("/")[-1]
            other_name = urllib.parse.unquote(each['article']['value'].split("/wiki/")[-1]).replace("_", " ")

            # skip those names not in english
            if len(set(other_name.lower()) - CONSTRAINS_CHARS) > 0:
                continue

            label_memo[node].add(other_name)
            if "(" in other_name and ")" in other_name:
                label_memo[node].add(other_name[:other_name.find("(")] + other_name[other_name.rfind(")") + 1:].strip())
        return label_memo

    def produce(self, input_file: str = None, input_df: pd.DataFrame = None,
                target_column: str = None, output_column_name: str = None, unique_columns=typing.List[str],
                column_metadata: dict = None) -> pd.DataFrame:
        """
        Main function of wikifier, the input could either be a dataframe or a input path
        """
        if column_metadata is None:
            column_metadata = {}
        level_restrict = column_metadata.get("context")
        if level_restrict in self.admin_level_mapping:
            self.level_restrict = self.admin_level_mapping[level_restrict]

        if input_file is None and input_df is None:
            raise ValueError("input_file and input_df can't both be None!")

        if target_column is None:
            raise ValueError("A target column name is needed!")

        if input_file is not None:
            input_df = pd.read_csv(input_file)
        else:
            temp_file_obj = tempfile.NamedTemporaryFile(mode='r+')
            input_df.to_csv(temp_file_obj, index=False)
            _ = temp_file_obj.seek(0)
            input_file = temp_file_obj.name

        # check if target column exists in input df
        if target_column not in input_df.columns:
            raise ValueError("Target column {} does not exist in input!".format(target_column))

        if self._is_census_code(input_df, target_column):
            output_df = self._add_census_wikifier_column(input_df, target_column)
        else:
            df_all = self.run_table_linker(input_file, target_column)
            final_answer = self.find_best_candidates(df_all)
            final_answer = Utility.sort_by_col_and_row(final_answer).reset_index().drop(columns=["index"])
            # return output
            output_df = input_df.copy()
            # update 2020.7.31, ensure index match
            output_df = output_df.reset_index()
            if output_column_name is None:
                output_column_name = "{}_wikifier".format(target_column)
            output_df[output_column_name] = final_answer["kg_id"]
            output_df = output_df.set_index("index")
            # clear level memo
            self.level_memo = defaultdict(int)
        return output_df

    def fetech_label(self, input_df: pd.DataFrame, target_column: str):
        """
            used for confirm whether the prediction is correct or not, just for development purpose
        """
        each_part_str = " ".join(["wd:{}".format(each) for each in input_df[target_column].dropna().unique()])
        query = """
        SELECT ?item ?itemLabel
        WHERE
        {{
          values ?item {{{q_nodes}}}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
        }}
        """.format(q_nodes=each_part_str)
        result = self.send_sparql_query(query, self.sparql_server)
        labels_map = {}
        for each in result:
            node = each['item']['value'].split("/")[-1]
            label = each['itemLabel']['value'].split("/")[-1]
            labels_map[node] = label
        input_df["{}_labels".format(target_column)] = input_df[target_column].map(labels_map)
        return input_df

    def get_candidates(self, input_file_path: str, target_column: str) -> pd.DataFrame:
        """
        Main query to get most candidates
        :param input_file_path: input file path
        :param target_column: target column name
        :return:
        """
        shell_code = """tl --url {} --index {} \
        canonicalize "{}" --csv -c "{}" --add-other-information \
        / clean -c label \
        / get-exact-matches -i -c label_clean \
        / get-phrase-matches -c label_clean -n 5 -p labels^5,aliases \
        / get-fuzzy-matches -c label_clean -n 5 -p labels^5,aliases \
        / normalize-scores -c retrieval_score \
        / drop-duplicate -c kg_id --keep-method exact-match --score-column retrieval_score_normalized""". \
            format(self.es_server, self.es_index,
                   input_file_path, target_column)

        res = Utility.execute_shell_code(shell_code)
        if res == "":
            raise ValueError("Executing first query error when running on {}!".format(input_file_path))
        res_io = StringIO(res)
        output_file = pd.read_csv(res_io, dtype=object)
        return output_file

    def get_candidates2(self, input_file_path: str) -> pd.DataFrame:
        """
        Second query used to search for those mention which have multiple words and no exact match,
        try to remove all punctuations and query again
        :param input_file_path:
        :return:
        """
        shell_code = """tl --url {} --index {} \
        clean "{}" -c label \
        / get-exact-matches -i -c label_clean \
        / get-phrase-matches -c label_clean -n 5 -p labels^5,aliases \
        / get-fuzzy-matches -c label_clean -n 5 -p labels^5,aliases \
        / normalize-scores -c retrieval_score \
        / drop-duplicate -c kg_id --keep-method exact-match --score-column retrieval_score_normalized""". \
            format(self.es_server, self.es_index, input_file_path)
        res = Utility.execute_shell_code(shell_code)
        if res == "":
            raise ValueError("Executing second query when running on {}!".format(input_file_path))
        res_io = StringIO(res)
        output_file = pd.read_csv(res_io, dtype=object)
        return output_file

    def remove_punctuation(self, input_str):
        words_processed = str(input_str).lower().translate(self.TRANSLATOR).split()
        return "".join(words_processed)

    def run_table_linker(self, input_file: str = None, target_column: str = None) -> pd.DataFrame:
        """
        Use table linker to get the candidates
        :param input_file: input file path
        :param target_column: target column name
        :return: table linker output dataframe
        """
        # run first query
        df = self.get_candidates(input_file, target_column)

        # run second query for those candidates which don't have exact match
        second_query_df_dict = {}
        count = 0
        for each_key, each_group in df.groupby(["column", "row"]):
            each_group_no_na = each_group.dropna()
            if len(each_group_no_na) == 0:
                temp = each_group.iloc[0, :5]
                temp['label'] = self.remove_punctuation(temp['label'])
                second_query_df_dict[count] = temp.to_dict()
                count += 1
            # no exact match found and we can combine multiple words
            elif "exact-match" not in set(each_group['method'].unique()) and len(each_group["label"].iloc[0].split(" ")) > 1:
                temp = each_group.iloc[0, :5]
                temp['label'] = self.remove_punctuation(temp['label'])
                second_query_df_dict[count] = temp.to_dict()
                count += 1

        second_query_df = pd.DataFrame.from_dict(second_query_df_dict, orient="index")
        cols = ["column", "row", "label", "||other_information||"]

        if len(second_query_df) > 0:
            second_query_df = second_query_df.sort_index()[cols]
            # combine 2 parts
            with tempfile.NamedTemporaryFile(mode='r+') as temp:
                second_query_df.to_csv(temp, index=False)
                _ = temp.seek(0)
                df_second_query = self.get_candidates2(temp.name)
            df_all = pd.concat([df_second_query, df])
            df_all = Utility.sort_by_col_and_row(df_all)
            return df_all
        else:
            return df

    def find_best_candidates(self, df_all: pd.DataFrame):
        """
        after get the candidates by running table linker, choose the best candidate as answer
        algorithm:
        1. If only one candidate -> use it
        2. If multiple:
          2.1. Check exact match (if we have exact match, use exact match first)
            2.1.1. If multiple exact match -> go to 2.2
            2.1.2. One exact match -> use it
          2.2. Check the admin2 (one level upper, if exist), find the same admin2 candidates.
            2.2.1. If multiple admin2 matched -> go to 2.3
            2.2.2. One admin2 match -> use it
            2.2.3. No match -> go to 2.4
          2.3. choose the smallest edit distance one
          2.4. If the input string has multiple words, try to remove punctuations (like ' and space), Then search again. Otherwise -> go 2.5.
          2.5. If still no match on admin2, use the highest similarity score one.
          2.6. If still not candidate, give up.
        :param df_all: pd.Dataframe
        :return:
        """
        # find the best match result
        output_df_list = []
        pending_results = pd.DataFrame()
        for each_value, each_group in df_all.groupby(["column", "row"]):
            # update 2020.7.27, if restrict on admin level, only consider corresponding candidates
            if self.level_restrict is not None:
                each_group = self.filter_levels(each_group, self.level_restrict)

            # no candidates
            if len(each_group.dropna()) == 0:
                temp = each_group.iloc[0, :]
                # temp_kg_id = "Q{}".format(temp["label"].lower())
                # while temp_kg_id in self.
                # temp["kg_id"] =
                output_df_list.append(temp.to_dict())
                continue

            each_group = each_group.dropna()

            # 1. If only one candidate -> use it
            if len(each_group) == 1:
                temp = each_group.iloc[0]
                output_df_list.append(temp.to_dict())
                self.check_level_information(temp)
                continue
            # if we have exact match, use exact match first
            elif "exact-match" in set(each_group['method'].unique()):
                exact_match_res = each_group[each_group["method"] == "exact-match"]
                # One exact match -> use it
                if len(exact_match_res) == 1:
                    temp = exact_match_res.iloc[0]
                    output_df_list.append(temp.to_dict())
                    self.check_level_information(temp)
                    continue
                # multiple exact match
                else:
                    # check those later
                    pending_results = pd.concat([pending_results, exact_match_res])
                    continue
            # no exact match, check other information
            else:
                # remove possible duplicate candidates first
                each_group = each_group.drop_duplicates(subset='kg_id', keep="first")
                input_label = each_group['label_clean'].iloc[0]
                has_high_similairty_candidates = False

                # update 2020.7.27: check similarity, and only apply when there do exist similarity is higher than threshold
                for each_candidate_labels in each_group['kg_labels']:
                    for each_label in each_candidate_labels.split("|"):
                        if each_label[0] == '"' and each_label[-1] == '"':
                            each_label = each_label[1:-1]
                        score = self.similarity_unit.similarity(input_label, each_label)
                        if score >= self.similarity_threshold:
                            has_high_similairty_candidates = True
                            break
                    if has_high_similairty_candidates:
                        break

                if has_high_similairty_candidates:
                    pending_results = pd.concat([pending_results, each_group])
                else:
                    temp = each_group.iloc[0, :]
                    empty_result = {'column':  temp['column'], 'row':  temp['row'],
                                    'label': temp['label'], '||other_information||': temp['||other_information||'],
                                    'label_clean': temp['label_clean'], 'kg_id': "", 'kg_labels': "",
                                    'method': 'exact-match', 'retrieval_score': '0.0',
                                    'retrieval_score_normalized': '0.0'}
                    output_df_list.append(empty_result)

        max_v = 0
        level = 0
        for k, v in self.level_memo.items():
            if v > max_v:
                level = k
                max_v = v

        if len(pending_results) > 0:
            for each_value, each_group in pending_results.groupby(["column", "row"]):
                # Check the extra information part
                possible_candidates = self.get_higher_score_candidate(each_group, keep_multiple_highest=True, level=level)
                possible_candidates = list(possible_candidates.values())[0]
                if len(possible_candidates) == 1:
                    output_df_list.append(possible_candidates[0].to_dict())
                    continue
                else:
                    # no way to figure out, use the higher retrieval_score one
                    highest_score = 0
                    final_res = None
                    for each in possible_candidates:
                        score = float(each["retrieval_score_normalized"])
                        if score > highest_score:
                            highest_score = score
                            final_res = each
                    output_df_list.append(final_res.to_dict())
                    continue

        output_df = pd.DataFrame(output_df_list)
        return output_df

    def check_level_information(self, match_candidate):
        level = max([len(each.split("<")) for each in match_candidate["kg_labels"].split("|")])
        self.level_memo[level] += 1

    def get_higher_score_candidate(self, match_res, keep_multiple_highest=False, level=None):
        filtered_level_res = pd.DataFrame()
        if level is not None:
            for _, each in match_res.iterrows():
                max_level = max([len(each.split("<")) for each in each["kg_labels"].split("|")])
                if max_level == level:
                    filtered_level_res = filtered_level_res.append(each)
            if len(filtered_level_res) > 0:
                match_res = filtered_level_res

        highest_score = -1
        res = []
        for _, each_row in match_res.iterrows():
            other_info = set(each.lower() for each in each_row["||other_information||"].split("|"))
            candidate_info = set(each.lower() for each in re.split(r'[,|<]', each_row['kg_labels']))
            score = len(candidate_info.intersection(other_info))
            if score > highest_score:
                res = [each_row]
                highest_score = score
            elif score == highest_score:
                res.append(each_row)
        if not keep_multiple_highest:
            return res[0]
        return {highest_score: res}

    def filter_levels(self, input_df: pd.DataFrame, level: int) -> pd.DataFrame:
        res = input_df[input_df["kg_labels"].apply(self._get_level) == level]
        if len(res) == 0:
            temp = input_df.iloc[0]
            empty_result = {'column': temp['column'], 'row': temp['row'],
                            'label': temp['label'], '||other_information||': temp['||other_information||'],
                            'label_clean': temp['label_clean'], 'kg_id': "", 'kg_labels': "",
                            'method': 'exact-match', 'retrieval_score': '0.0',
                            'retrieval_score_normalized': '0.0'}
            empty_output = pd.DataFrame([empty_result])
            return empty_output
        else:
            return res

    def _get_level(self, s: str):
        if isinstance(s, str):
            return max([len(each.split("<")) for each in s.split("|")])
        else:
            return 0

    def _is_census_code(self, input_df: pd.DataFrame, target_column: str) -> bool:
        self.census_qnode = []
        count = 0
        for _, val in input_df[[target_column]].iterrows():
            qnode = ethiopia_census_code.get(val.iloc[0], '')
            self.census_qnode.append(qnode)
            if qnode:
                count += 1
        return  count / len(self.census_qnode) > 0.9

    def _add_census_wikifier_column(self, input_df: pd.DataFrame, target_column: str):
        input_df[f'{target_column}_wikifier'] = self.census_qnode
        return input_df

def test_run():
    test = EthiopiaWikifier()
    # kgtk_file = "/Users/minazuki/Desktop/vector/woreda_wikifier/region-ethiopia-exploded-edges.tsv"
    # output_path = "/Users/minazuki/Desktop/vector/woreda_wikifier/region-ethiopia-exploded-edges_out.tsv"
    input_file_path = "~/Desktop/vector/woreda_wikifier/ethiopia_regions.csv"
    target_column = "admin3"
    result = test.produce(input_file=input_file_path, target_column=target_column, output_column_name=None)
    wrong_res = pd.DataFrame()
    for _, each_row in result.iterrows():
        if each_row["admin3_wikifier"] != each_row["admin3_id"]:
            wrong_res = wrong_res.append(each_row)
    print("wrong_res")
    print(wrong_res)
    return result


def encode(input_df: pd.DataFrame, target_col: str):
    """
    designed to encode the input dataframe to reduce the dataframe size, need more further development
    :param input_df:
    :param target_col:
    :return:
    """
    import hashlib
    memo = {}
    new_res = []
    input_df = input_df.fillna("")
    for i, each_row in input_df.iterrows():
        target_val = each_row[target_col]
        # target_val = "|".join(sorted(list(set(target_val.split("|")))))
        # each_row[target_col] = target_val
        if len(target_val) > 10:
            hash_generator = hashlib.md5()
            hash_generator.update(target_val.encode('utf-8'))
            hash_search_result = hash_generator.hexdigest()
            hash_key = str(hash_search_result)
            if hash_key in memo:
                each_row[target_col] = "${}$".format(memo[hash_key])
            else:
                memo[hash_key] = "{}".format(i)
        new_res.append(each_row)
    output_df = pd.DataFrame(new_res)
    return output_df
