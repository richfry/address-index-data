#!/usr/bin/env python
"""
ONS Address Index - Automatic Testing of Different Datasets
===========================================================

A simple wrapper to call all independent address linking datasets in serial.
Could be parallelised trivially, however, the linking code is memory hungry
so the node needs to have sufficient memory to enable this.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python automatic_testing.py


Requirements
------------

:requires: pandas (tested with 0.19.1)
:requires: matplotlib (tested with 1.5.3)
:requires: sqlalchemy (tested with 1.1.4)
:requires: addressLinking (and all the requirements within it)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 1-Dec-2016

"""
import os
import datetime
import sqlite3
import Analytics.prototype.welshAddresses as wa
import Analytics.prototype.landRegistryAddresses as lr
import Analytics.prototype.edgeCaseAddresses as ec
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from Analytics.linking import addressLinking


# set global location variable that is platform specific so that there is no need to make code changes
if 'Pro.local' in os.uname().nodename:
    location = '/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/'
elif 'cdhut-d03-' in os.uname().nodename:
    location = '/opt/scratch/AddressIndex/Results/'
else:
    raise ConnectionError('ERROR: cannot connect to the SQLite3 database')


def run_all_datasets():
    """
    Run all address linking codes in serial.

    :return: None
    """
    print('Running Edge Case addresses test...')
    ec.run_edge_case_linker()

    print('Running Welsh addresses test...')
    wa.run_welsh_address_linker()

    print('Running Landry Registry addresses test...')
    lr.run_land_registry_linker()


def _load_welsh_data():
    """
    Load Welsh address data and results. Joint the information together to a single
    dataframe.

    :return: a single data frame containing original data and attached UPRNs
    :rtype: pandas.DataFrame
    """
    # load original data
    original = pd.read_csv(location + 'WelshGovernmentData21Nov2016.csv',
                           usecols=['ID', 'UPRNs_matched_to_date'])
    original.rename(columns={'UPRNs_matched_to_date': 'UPRN_ORIG'}, inplace=True)

    # load prototype linked data
    prototype = pd.read_csv(location + 'WelshGov_matched.csv',
                            usecols=['ID', 'UPRN'])
    prototype.rename(columns={'UPRN': 'UPRN_PROTO'}, inplace=True)

    # load SAS code (PG) data
    sas = pd.read_csv(location + 'Paul_matches_with_address_text_welshGov.csv',
                      usecols=['UID', 'UPRN'])
    sas.rename(columns={'UID': 'ID', 'UPRN': 'UPRN_SAS'}, inplace=True)

    # join data frames
    data = pd.merge(original, prototype, how='left', on='ID')
    data = pd.merge(data, sas, how='left', on='ID')

    return data


def _compute_welsh_performance(df, methods=('UPRN_ORIG', 'UPRN_PROTO', 'UPRN_SAS')):
    """
    Compute performance for the Welsh dataset using SAS code UPRNs as a reference.

    :param df: dataframe containing UPRNs of methods as columns
    :type df: pandas.DataFrame
    :param methods: a tuple listing methods to analyse
    :type methods: tuple

    :return: results of the performance computations
    :rtype: dict
    """
    # simple performance metrics that can be computed directly from the data frame and dummies

    msk = df['UPRN_PROTO'].isnull()
    addresses = len(df.index)
    linked = len(df.loc[~msk].index)
    not_linked = len(df.loc[msk].index)

    msk = df['UPRN_SAS'].isnull()
    withUPRN = len(df.loc[~msk].index)

    correct = -1
    false_positive = -1
    new_UPRNs = -1

    # iterate over the possible method combinations - capture relevant information
    for i, method1 in enumerate(methods):
        for j, method2 in enumerate(methods):
            if method1 == 'UPRN_SAS' and method2 == 'UPRN_PROTO':
                agree = df[method1] == df[method2]
                nagree = len(df.loc[agree].index)

                msk = (~df[method1].isnull()) & (~df[method2].isnull())
                disagree = df.loc[msk, method1] != df.loc[msk, method2]
                ndisagree = len(df.loc[msk & disagree].index)

                msk = (df[method1].isnull()) & (~df[method2].isnull())
                nmethod2only = len(df.loc[msk].index)

                correct = nagree
                false_positive = ndisagree
                new_UPRNs = nmethod2only

    results = dict(addresses=addresses, correct=correct, false_positive=false_positive, linked=linked,
                   new_UPRNs=new_UPRNs, not_linked=not_linked, withUPRN=withUPRN)

    return results


def compute_performance():
    """
    Computes additional performance metrics as some datasets have multiple UPRNs attached or
    UPRNs have been attached later.

    :return: None
    """
    welsh_data = _load_welsh_data()

    # compute results and create a dictionary
    results = _compute_welsh_performance(welsh_data, methods=('UPRN_PROTO', 'UPRN_SAS'))
    results['code_version'] = addressLinking.__version__
    results['dataset'] = 'WelshGovernmentData21Nov2016.csv'
    results['date'] = datetime.datetime.now()
    results['name'] = 'WelshGovSAS'

    # convert to Pandas Dataframe
    results = pd.DataFrame.from_records([results])

    # push to the database
    with sqlite3.connect(location + 'AddressLinkingResults.sqlite') as cnx:
        results.to_sql('results', cnx, index=False, if_exists='append')


def _get_data_from_db(sql):
    """
    Pull data from a database.

    :param sql: sql query to execute to pull the data
    :type sql: str

    :return: queried data
    :rtype: pandas.DataFrame
    """
    # build the connection string from specifying the DB type, location, and filename separately
    connection = 'sqlite:///' + location + 'AddressLinkingResults.sqlite'

    df = pd.read_sql_query(sql, create_engine(connection))

    return df


def _create_figures(plot_data, testset_name, columns_to_plot):
    """
    Create two figures to show the performance as a function of time.

    :param plot_data: dataframe contaninig column date and those to be plotted
    :type plot_data: pandas.DataFrame
    :param testset_name: name of the test dataset, used as a part of the output file name
    :type testset_name: str
    :param columns_to_plot: a list of names of the columns storing the performance metrics to be plotted
    :type columns_to_plot: list

    :return: None
    """
    plot_data.plot(x='date', y=columns_to_plot,
                   subplots=True, sharex=True, layout=(3, 2), figsize=(12, 18),
                   fontsize=16, sort_columns=True, color='m',
                   xlim=(plot_data['date'].min() - datetime.timedelta(days=1),
                         plot_data['date'].max() + datetime.timedelta(days=1)))
    plt.tight_layout()
    plt.savefig(location + testset_name + 'results.png')
    plt.close()

    plot_data.plot(x='date', y=columns_to_plot,
                   figsize=(12, 18), fontsize=16, sort_columns=True,
                   xlim=(plot_data['date'].min() - datetime.timedelta(days=1),
                         plot_data['date'].max() + datetime.timedelta(days=1)),
                   ylim=(plot_data[columns_to_plot].min(axis=0).min() - 1,
                         plot_data[columns_to_plot].max(axis=0).max() + 1))
    plt.tight_layout()
    plt.savefig(location + testset_name + 'results2.png')
    plt.close()


def plot_performance():
    """
    Generates simple graphs which show the linking performance as a function of time for all datasets
    available from the results database.

    For each dataset two graphs are generated: 1) figure with multiple sub-figures, and 2)
    a single figure showing multiple lines.

    :return: None
    """
    # query data and place it to a Pandas DataFrame
    data = _get_data_from_db('select * from results;')

    # convert date to datetime
    data['date'] = pd.to_datetime(data['date'])

    # create figures
    for testset_name in set(data['name']):
        plot_data = data.loc[data['name'] == testset_name]
        print('Plotting {} results'.format(testset_name))
        _create_figures(plot_data, testset_name,
                        ['addresses', 'correct', 'false_positive', 'linked', 'new_UPRNs', 'not_linked'])


def run_all(plot_only=False):
    """
    Execute the fully automated testing sequence.

    :param plot_only: whether to re-run all test datasets or simply generate performance figures
    :param plot_only: bool

    :return: None
    """
    if not plot_only:
        run_all_datasets()
        compute_performance()
    plot_performance()


if __name__ == "__main__":
    run_all()