"""
Create ElasticSearch Hybrid Index
=================================

A simple script to create an Elastic Search index from an AddressBase Hybrid CSV file.
The script does not define mappings or any specific configuration. Can therefore be
used for testing only.


Running
-------

After all requirements are satisfied and the required files are available,
the script can be invoked using CPython interpreter::

    python createElasticIndex.py


Requirements
------------

:requires: elastisearch (2.4.0)
:requires: pandas (0.19.1)
:requires: numpy (1.11.2)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 31-Jan-2017
"""
import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch


def load_data(filename='AB_processed.csv', path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/',
              test=False):
    """
    Load AddressBase data to a Pandas DataFrame.
    As a quick fix, fill nones

    :param filename: name of the CSV file to load AB from
    :type filename: str
    :param path: location of the AB file
    :type path: str
    :param test: whether or not to populated the test index
    :type test: bool

    :return: AddressBase
    :type: pandas.DataFrame
    """
    if test:
        filename = 'ABtest.csv'
        dtype = {'UPRN': np.int64, 'POSTCODE_LOCATOR': str, 'ORGANISATION_NAME': str,
                 'DEPARTMENT_NAME': str, 'SUB_BUILDING_NAME': str, 'BUILDING_NAME': str,
                 'BUILDING_NUMBER': str, 'THROUGHFARE': str, 'DEPENDENT_LOCALITY': str,
                 'POST_TOWN': str, 'POSTCODE': str, 'PAO_TEXT': str,
                 'PAO_START_NUMBER': np.float64, 'PAO_START_SUFFIX': str, 'PAO_END_NUMBER': np.float64,
                 'PAO_END_SUFFIX': str, 'SAO_TEXT': str, 'SAO_START_NUMBER': np.float64,
                 'SAO_START_SUFFIX': str, 'ORGANISATION': str, 'STREET_DESCRIPTOR': str,
                 'TOWN_NAME': str, 'LOCALITY': str}
    else:
        dtype = {'UPRN': np.int64, 'ORGANISATION_NAME': str, 'PAO_END_SUFFIX': str,
                 'DEPARTMENT_NAME': str, 'SUB_BUILDING_NAME': str, 'BUILDING_NAME': str,
                 'BUILDING_NUMBER': str, 'THROUGHFARE': str,
                 'POST_TOWN': str, 'POSTCODE': str, 'PAO_TEXT': str,
                 'PAO_START_NUMBER': np.float64, 'PAO_START_SUFFIX': str,
                 'SAO_TEXT': str, 'SAO_START_NUMBER': np.float64, 'SAO_START_SUFFIX': str,
                 'STREET_DESCRIPTOR': str, 'TOWN_NAME': str, 'LOCALITY': str,
                 'postcode_in': str, 'postcode_out': str}

    data = pd.read_csv(path + filename, dtype=dtype)
    print('\nFound {} addresses from AddressBase...'.format(len(data.index)))
    print(data.info())

    data.fillna('', inplace=True)

    return data


def create_and_populate_index(data, index_name='addressbase', type_name='hybrid'):
    """
    Creates and populates a new index with given data.

    If an index exists then deletes the old one before creating a new one.
    Converts the given dataframe to lists with dictionaries for bulk population.
    This is not very efficient as requires iterating over the data.

    :param data: input data to be indexed
    :type data: pandas.DataFrame
    :param index_name: name of the index to create
    :type index_name: str
    :param type_name: name of the type
    :type type_name: str

    :return: None
    """
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

    if es.indices.exists(index_name):
        print('Index exists, deleting old...')
        res = es.indices.delete(index=index_name)
        print('Response:', res)

    request_body = {'settings': {'number_of_shards': 1, 'number_of_replicas': 0}}
    print('\nCreating index', index_name)
    res = es.indices.create(index=index_name, body=request_body)
    print('Response:', res)

    bulk_data = []
    for index, row in data.iterrows():
        data_dict = {}
        for i in range(len(row)):
            data_dict[data.columns[i]] = row[i]
        op_dict = {
            "index": {
                "_index": index_name,
                "_type": type_name,
                "_id": data_dict['UPRN']
            }
        }
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)

    print('\nPopulating data...')
    res = es.bulk(index=index_name, body=bulk_data, refresh=True)


def test(index_name='addressbase'):
    """
    Run a simple match_all query with size = 1 to check that something has been populated.
    to the given index.

    :param index_name: name of the index to query
    :type index_name: str

    :return: None
    """
    print('\nTesting index...')
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
    res = es.search(index=index_name, size=1, body={'query': {'match_all': {}}})
    print('Response:', res)


def test2(index_name='addressbase'):
    """
    Run a simple boolean query to test that the index has been populated.

    :param index_name: name of the index to query
    :type index_name: str

    :return: None
    """
    print('\nTesting index...')
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
    res = es.search(index=index_name, body={'query': {'bool': {'should': [{'match': {'POSTCODE': 'CF48 2NQ'}},
                                                                          {'match': {'BUILDING_NAME': 'PENLAN'}}],
                                                               "minimum_should_match": 1,
                                                               'boost': 1.0}}})
    for hit in res['hits']['hits']:
        print(hit)


if __name__ == '__main__':
    data = load_data()
    create_and_populate_index(data)
    test()
    test2()
