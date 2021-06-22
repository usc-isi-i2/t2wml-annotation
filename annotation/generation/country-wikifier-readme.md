# Adding new entries
New entries need to be added to both country-wikifier.csv and country_wikifier_cache.json files.

For example, to add NATO (Q7184):

country-wikifier.csv
```
,,NATO,,Q7184
```

country_wikifier_cache.json:
```
{
  ...
  
  "528": "Q29999",
  "nato": "Q7184"
}
```

Also, make sure the entry label edge is added to Datamart
```
Q7184 label 'NATO'@en
```
