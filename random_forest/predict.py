import csv
import numpy as np
import random
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib

with open('newfile.csv', 'rb') as h:
    reader = csv.reader(h)
    title = reader.next()
    F = []
    for row in reader:
        F.append(row)
    random.shuffle(F)
    X = np.asarray(F)
    Y = X[:, 0]
    X = X[:, 1:]
    (m, n) = X.shape
    for i in range(m):
        if X[i, 23] == '':
            X[i, 23] = '-1'
        if X[i, 24] == '':
            X[i, 24] = '-1'
        if X[i, 27] == '':
            X[i, 27] = '-1'
        if X[i, 30] == '':
            X[i, 30] = '-1'
        if X[i, 31] == '':
            X[i, 31] = '-1'
        if X[i, 33] == '':
            X[i, 33] = '-1'
        if X[i, 37] == '':
            X[i, 37] = random.choice('01')
        if X[i, 38] == '':
            X[i, 38] = '-1'
        for j in range(n):
            try:
                X[i, j] = float(X[i, j])
            except ValueError:
                X[i, j] = '0'
    X = X.astype(np.float)
    Y = Y.astype(np.int)
    x_test = X
    y_test = Y

    print "start predict:"
    clf = joblib.load('tree.pkl')
    pre = clf.predict_proba(x_test)
    print pre[:, 1]