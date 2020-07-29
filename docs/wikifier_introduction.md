## Wikifier introudction
Currently there are 2 different wikifier available in t2wml-annotation: country wikifier and ethiopia wikifier.
### Country wikifier
This is a simple wikifier which designed to wikify only for countries. The input could be a country full name, short name or country code.

#### Algorithm
This wikifier simply has 2 part of searching algorithm:
1. If the input label has exact match to the label, just return the result.
2. Otherwise, run the similarity function (currently it is `Hybrid Jaccard` similarity) to compute the similairt of input string to other labels, find the highest similarity candidate, depending on the similarity score, return this candidate or return empty match.
#### API
This wikifier has a very simple api. To use, please follow this example:
The input should be a list of string and the output will be a dict, where the key is the distinct value of the input list and the value is the target Q node.
```python
>>> from annotation.generation.country_wikifier import DatamartCountryWikifier
>>> wikifier = DatamartCountryWikifier()
>>> result = wikifier.wikify(["portugal","portugel", "partugel"])
`portugel` not in record, will try to find the closest result
`partugel` not in record, will try to find the closest result
Not wikify for input value `partugel`
>>> result
{'portugal': 'Q45', 'portugel': 'Q45', 'partugel': None}
>>>
```
#### Search support content
For example: `Q45` is the country Q node of `portugal` ,
It is also available when search for `portuguese republic`(official name), `pt`(short name), `prt` (short name), `620` (country code).
The wikifier also support fuzzy search, so if the input string is `portugel`(a typo), the system can still find the target Q node. 

#### Threshold
Currently there is a tollerance (similarity threshold) on the fuzzy search, if the given string similairty to any candidate labels are lower than threshold, the system will treat as no candidate.

For example, as mentioned above, if the search target is `portugel`, the Hybrid Jaccard similarity between `portugel` and `protugal` is `0.95` which is higher than threshold we can find the result.
If the search target is `partugel`, which similarity to `protugal` is `0.85`

#### File
This country wikifier depend on a  cache file stored at the same location of the code named `country_wikifier_cache.json`. This ia a smple json structure file with a one-level key, value pair structure.

### Ethiopia wikifier
This wikifier runs depending on package [table-linker](https://github.com/usc-isi-i2/table-linker). Currently this wikifier service need to access a elastic search server (tested on version)`5.6.14` as a query database and a wikidata sparql query (like offical wikidata one). 

Default, it will use elastic search index stored at `kg2018a.isi.edu:9200/ethiopia_wikifier_index`(VPN needed) and sparql query endpoint at `https://dsbox02.isi.edu:8888/bigdata/namespace/wdq/sparql`.
It also support to change those address to others if necessary.

This wikifier basic algorithm should be adaptable to all other "3-level" location structures in other countries in the world. With the update of the input kgtk file, it is possible to used for wikification on other locations.

#### Algorithm
1. The wikifier first use table linker functions, to create a canonical file, with cleaned labels.
2. Then, use table-linker to search candidates, currently 3 searching algorithm is used: `exact-match`, `phrase-match` and `fuzzy-match`.
3. Based on the candidates returned from table-linker, the wikifier will choose the best candidate based on following logistic:
```
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
```
#### API
This wikifier has a more option support for tuning, for example:
```python
>>> from annotation.generation.ethiopia_wikifier import EthiopiaWikifier
# here all those 4 parameters are optional
>>> wikifier = EthiopiaWikifier(es_server=None, es_index=None, sparql_server=None, similarity_threshold = 0.5) 
# assume we have an input dataframe looks like:
>>> input_df
      col1            col2
0  Gambela  something else
1   oromia  something else
# we want to wikify first col, so the input_col_name should be "input_col_name"
>>> input_col_name = "col1"
# column_metadata is an optional dict, currently it support setting the level constrains on searching results, for example, if you want to get the candidates based only on "admin1" level, set it as:
>>> column_metadata = {"context" : "admin1"}
# output column name is also an optional parameter. if not given, default the output column name will be input_col_name + "_wikifier"
>>> result1 = wikifier.produce(input_df=input_df, target_column=input_col_name, column_metadata=column_metadata)
>>> result1
      col1            col2 col1_wikifier
0  Gambela  something else       Q207638
1   oromia  something else       Q202107
# wikifier also support to send a path to csv file directly like:
# one thing need to notice is here the path must need to be a absolute path to the file
>>> result2 = wikifier.produce(input_file="input_file.csv", target_column=input_col_name, column_metadata=column_metadata)
# The output will be exact same as result1
```
For details, 
`EthiopiaWikifier` class support 4 **optional** initialization parameters:
- es_server {string} : The elastic search server port, for example: `http://kg2018a.isi.edu:9200`
- es_index {string}: The elastic search index need to be used, for example: `ethiopia_wikifier_index`
- sparql_server {string}: The sparql query endpoint of wikidata, for example: `https://dsbox02.isi.edu:8888/bigdata/namespace/wdq/sparql`
- similarity_threshold {float}: The minimum similarity required for matching the candidates to input. If the candidate node's label similarity to the target input label is lower, those candidates will not be considered.
`produce()` function suppport following parameters:
- input_file {string}: The path to the input csv file.
- input_df {pd.DataFrame}: The input object of a pandas dataframe.
- target_column {string}: The target column name needed to be wikified.
- output_column_name {string}: *optional*, The output wikifier results column name.
- column_metadata {dict}: *optional*, currently it only support constrains on place level.

Here at least one parameter is required from `input_file` or `input_df`

#### File
There is a [kgtk](https://github.com/usc-isi-i2/kgtk) edges file `region-ethiopia-exploded-edges.tsv` stored at the same location of the code. This kgtk file stored all recorded indexes. For how to update this index file, please refer 
to `index update` section.

The structure of the kgtk file is simple, it contains 3 basic columns: `node1`, `label` and `node2`.
For example:
node1   |label| node2
-------|-----------|------
Q3120775 |   P31| Q13221722
Q3120775|    label|   Gulomahda
Q3120775|    P17| Q115
Q3120775|    P2006190001| Q200127
Q3120775|    P2006190002| Q3316357
Q3120775|    P2006190003| Q3120775
Following edges are **REQUIRED**:
- `label`: The label of the node is the most important edge. It is used for building the index for searching. **Multiple** `label` edges is allowed.
- Depending on the node location level: available in {`admin1`(or state), `admin2` (or county), `admin3`(or city / area)}, follow edges needed:
- - If this is a `admin1` node, `P2006190001` is needed, and the value of node2 is itself.
- - If this is a `admin2` node, `P2006190001`  and `P2006190002` are needed, the value of `P2006190001`  should be the value of the `admin1` node of this `admin2` node. The value of `P2006190002` should be itself.
- - If this is a `admin2` node, `P2006190001` , `P2006190002` and `P2006190003` are needed, the value of `P2006190001`  should be the value of the `admin1` node, the value of `P2006190002` should be the value of the `admin2` node. The value of `P2006190003` should be itself.

Following edges are optional:
- `P31`: The `instance of` edge, currently this node is not used.
- `P17`: The `country` edge, currently this node is not used.

#### Index update
If in the future, more location related nodes were found and the index in elastic search need to be updated, there is an index updating api. To use this, please follow the example below:

```python
>>> from annotation.generation.ethiopia_wikifier import EthiopiaWikifier
>>> wikifier = EthiopiaWikifier(es_server=None, es_index=None, sparql_server=None, similarity_threshold = 0.5) 
>>> input_index_kgtk_file = "input_index_kgtk.csv"
>>> wikifier.upload_to_es(input_index_kgtk_file)
```

### TODO
#### Error handling on table linker
Currently if table linker pipeline failed or get some error during running, the error message from table linker side is incorrect. The error message will always from the last step, however, it is very possible something went wrong before. So we need to improve table linker algorithm to get a better error handle.
#### Run as a service
If possible, we should run thie wikifier service in a REST api in the future, so that we can enable user not have to access a elastic search index or use VPN to using the wikifier service.