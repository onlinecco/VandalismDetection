from sklearn.ensemble import RandomForestClassifier
from sklearn.decomposition import PCA
import numpy as np
import pandas as pd
from numpy import genfromtxt, savetxt
import random


dataset = genfromtxt(open('data.csv','r'), delimiter=',', dtype='f8' ,filling_values='0')


target = [x[0] for x in dataset]
tups = np.array([x[1:] for x in dataset])
pca = PCA()
pca.fit(tups)
pca.transform(tups)
train_size = int(0.8 * len(tups))
train_nums = set()
train = []
train_target = []
for i in range(train_size):
    num = random.randint(0, len(tups) - 1)
    while num in train_nums:
        num = random.randint(0, len(tups) - 1)
    train_nums.add(num)
    train.append(tups[num])
    train_target.append(target[num])

test_nums = set([i for i in range(len(tups))]).difference(train_nums)
testval = [target[i] for i in test_nums]
testtrain = [tups[i] for i in test_nums]

rf = RandomForestClassifier(n_estimators=100)
rf.fit(train, train_target)

score = rf.score(testtrain,testval)
print 'the score is ', score
