import numpy as np
import pandas
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import Imputer


def get_data_chunk(num_revisions=None, skiprows=None):
    data = pandas.read_csv('features.csv', sep=',', nrows=num_revisions,
                            skiprows=skiprows,
                            false_values=['F', 'f', 'False', 'false'],
                            true_values=['T', 't', 'True', 'true'],
                            parse_dates=True)

    return data


def process_chunk(data):
    vec = DictVectorizer()
    result_key = 'rollbackReverted'

    # Extract results from data and remove it
    Y = data[result_key].values
    del data[result_key]

    # Convert data to proper format
    # Transform non numerical data
    lists_of_dicts = data.T.to_dict().values()
    X = vec.fit_transform(lists_of_dicts)


    return X, Y


def fix_unknowns(data_chunk):
    # Replace unknown values with mean of feature
    imp = Imputer(strategy='mean', axis=0)
    data_chunk = imp.fit_transform(data_chunk.toarray())

    return data_chunk
