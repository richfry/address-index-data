"""
Test ElasticSearch Address Linking
==================================

A simple script to test ElasticSearch based AddressLinking.



Running
-------

After all requirements are satisfied and the required index is available,
the script can be invoked using CPython interpreter::

    python testElasticSearchLinking.py


Requirements
------------

:requires: elastisearch (2.4.0)
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
from elasticsearch import Elasticsearch
from Analytics.linking import addressLinking


def test_elastic_search_linking(index_name='addressbasetest'):
    """
    A simple function to run through the test dataset.

    :param index_name: name of the index against which the queries are run
    :type index_name: str

    :return: None
    """
    settings = dict(test=True)

    # load test data and parse using Address Linker method
    ip = addressLinking.AddressLinker(**settings)
    ip.load_data()
    ip.parse_input_addresses_to_tokens()

    # get the data and store existing UPRN
    data = ip.toLinkAddressData
    UPRN = data['UPRN_old'].values
    new_uprn = []

    # create connection to the local Elastic
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

    # loop over data and execute a query
    for index, row in data.iterrows():

        shoulds = []

        if row['OrganisationName'] is not None:
            shoulds.append({'match': {'ORGANISATION_NAME': row['OrganisationName']}})

        if row['DepartmentName'] is not None:
            shoulds.append({'match': {'DEPARTMENT_NAME': row['DepartmentName']}})

        if row['SubBuildingName'] != 'N/A':
            shoulds.append({'match': {'SUB_BUILDING_NAME': row['SubBuildingName']}})

        if row['BuildingName'] is not None:
            shoulds.append({'match': {'BUILDING_NAME': row['BuildingName']}})
            shoulds.append({'match': {'PAO_START_NUMBER': row['BuildingName']}})

        if row['BuildingNumber'] is not None:
            shoulds.append({'match': {'BUILDING_NUMBER': row['BuildingNumber']}})

        if row['StreetName'] is not None:
            shoulds.append({'match': {'THROUGHFARE': row['StreetName']}})
            shoulds.append({'match': {'STREET_DESCRIPTOR': row['StreetName']}})

        if row['TownName'] is not None:
            shoulds.append({'match': {'POST_TOWN': row['TownName']}})
            shoulds.append({'match': {'TOWN_NAME': row['TownName']}})

        if row['Postcode'] is not None:
            shoulds.append({'match': {'POSTCODE': row['Postcode']}})

        if row['Locality'] is not None:
            shoulds.append({'match': {'LOCALITY': row['Locality']}})

        if row['BuildingSuffix'] != 'N/A':
            shoulds.append({'match': {'PAO_START_SUFFIX': row['BuildingSuffix']}})

        if row['BuildingStartNumber'] is not None:
            shoulds.append({'match': {'PAO_START_NUMBER': row['BuildingStartNumber']}})

        if row['PAOText'] is not None and row['PAOText'] != '':
            shoulds.append({'match': {'PAO_TEXT': row['PAOText']}})

        if row['SAOText'] != 'N/A':
            shoulds.append({'match': {'SAO_TEXT': row['SAOText']}})

        if row['FlatNumber'] != -12345:
            shoulds.append({'match': {'SAO_START_NUMBER': row['FlatNumber']}})

        query_body = {'query': {'bool': {'should': shoulds, "minimum_should_match": '55%', 'boost': 1.0}},
                      'sort': {'_score': 'desc'}}

        res = es.search(index=index_name, body=query_body)

        uprn_int = int(res['hits']['hits'][0]['_id'])
        new_uprn.append(uprn_int)
        if uprn_int != row['UPRN_old']:
            print('\n\n')
            print(query_body)
            print(row['UPRN_old'])
            print(res['hits']['hits'])

    new_uprn = np.asarray(new_uprn)

    np.testing.assert_array_equal(UPRN, new_uprn, verbose=True)


if __name__ == "__main__":
    test_elastic_search_linking()
