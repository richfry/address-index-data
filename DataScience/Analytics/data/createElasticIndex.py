"""

"""
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch


def load_data(filename='ABtest.csv', path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/'):
    data = pd.read_csv(path + filename,
                       dtype={'UPRN': np.int64, 'POSTCODE_LOCATOR': str, 'ORGANISATION_NAME': str,
                              'DEPARTMENT_NAME': str, 'SUB_BUILDING_NAME': str, 'BUILDING_NAME': str,
                              'BUILDING_NUMBER': str, 'THROUGHFARE': str, 'DEPENDENT_LOCALITY': str,
                              'POST_TOWN': str, 'POSTCODE': str, 'PAO_TEXT': str,
                              'PAO_START_NUMBER': str, 'PAO_START_SUFFIX': str, 'PAO_END_NUMBER': str,
                              'PAO_END_SUFFIX': str, 'SAO_TEXT': str, 'SAO_START_NUMBER': np.float64,
                              'SAO_START_SUFFIX': str, 'ORGANISATION': str, 'STREET_DESCRIPTOR': str,
                              'TOWN_NAME': str, 'LOCALITY': str})
    print('Found {} addresses from AddressBase...'.format(len(data.index)))

    data.fillna('', inplace=True)

    return data


def create_index(data, index_name='addressbase', type_name='hybrid'):
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

    if es.indices.exists(index_name):
        print('Index exists, deleting old...')
        res = es.indices.delete(index=index_name)
        print('Response:', res)

    request_body = {'settings': {'number_of_shards': 1, 'number_of_replicas': 0}}
    print('Creating index', index_name)
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

    print('Populating data...')
    res = es.bulk(index=index_name, body=bulk_data, refresh=True)
    # print(res)


def test(index_name='addressbase'):
    print('Testing index...')
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
    res = es.search(index=index_name, size=1, body={'query': {'match_all': {}}})
    print('Response:', res)


if __name__ == '__main__':
    data = load_data()
    create_index(data)
    test()
