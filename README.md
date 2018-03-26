#### Twenty Million Product - Spark to ElasticSearch

This project join two big csv files using spark and save it to elastic search.

Directory structure:
- fact: Fact class to handle fact.csv;
- product_dimension: Product Dimension class to handle dim.csv;
- main: Calls the spark classes and save to the elasticsearch.
- requirements: Python requirements (python 3.6)

Executing:
- Create a virtualenv with python 3.6 to install the requirements;
- Execute main.py (it will load data to a local elasticsearch)

Pending tasks:
- Unload json to disk before sending to elasticsearch (collect from memory raises OOM)