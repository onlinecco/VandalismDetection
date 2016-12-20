import csv
import numpy as np
import random
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import f_regression
from sklearn.feature_selection import SelectFromModel
from sklearn.metrics import roc_curve
from sklearn.metrics import roc_auc_score
from sklearn import tree
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression

def score():
	f = open("socres", 'w')
	for i in range(len(p)):
		f.write("%s %s %s\n"%(p[i][0], p[i][1], testing_data[i, 0]))
	f.close()
	file = open("socres", "r")
	data = file.readlines()
	file.close()
	
	label = []
	score = []
	
	for line in data:
		line = line.strip()
		line = line.split(" ")
		score.append(float(line[1]))
		label.append(int(float(line[2])))
	label = np.array(label)
	score = np.array(score)
	
	fpr, tpr, thresholds = roc_curve(label, score, pos_label=1)
	print(roc_auc_score(label, score))

def accuracy(p, results, testing_data):
	counter = 0
	error_0 = 0
	error_1 = 0
	valCtr = 0
	nonValCtr = 0
	
	for i in range(0, len(testing_data)):
		if testing_data[i, 0] == 0:
			nonValCtr += 1
		if testing_data[i, 0] == 1:
			valCtr += 1
		if testing_data[i, 0] == results[i]:
			counter += 1
		else:
			if testing_data[i, 0] == 0:
				error_0 += 1
			else:
				error_1 += 1
			
	
	print(results)
	print("accuracy is ", counter/len(testing_data))
	print("error on not val ", error_0/nonValCtr)
	print("error on val ", error_1/valCtr)

with open('all.csv', 'rt') as csvfile:
	reader = list(csv.reader(csvfile))
	cols = len(reader[0])
	counetrs = [0] * cols
	headers = []
	for row_number in range(0, len(reader)):
		row = reader[row_number]
		if row_number == 0:
			headers = row
		for i in range(1, len(row)):
			if row[i] == '':
				reader[row_number][i] = np.nan
				counetrs[i] += 1
for i in range(0, cols):
	if counetrs[i] > 30000:
		print(headers[i])
random.shuffle(reader)
idx = len(reader) * 0.8
features = np.array(reader)
features = features[1:, :]
imp = preprocessing.Imputer(missing_values = 'NaN', strategy = 'median', verbose = 0)
imp.fit(features)
features = imp.transform(features)

#select = SelectKBest(f_regression, k=40).fit(features[:, 1:], features[:, 0])
#x = select.transform(features[:, 1:])
#features = np.column_stack((features[:, 0], x))

training_data = features[:idx, :]

with open('out.csv', 'rt') as csvfile:
	reader = list(csv.reader(csvfile))
	cols = len(reader[0])
	counetrs = [0] * cols
	headers = []
	for row_number in range(0, len(reader)):
		row = reader[row_number]
		if row_number == 0:
			headers = row

features = np.array(reader)
features = features[1:, :]
imp = preprocessing.Imputer(missing_values = 'NaN', strategy = 'median', verbose = 0)
imp.fit(features)
features = imp.transform(features)

testing_data = features[idx:, :]
#random forest with bagging
'''
clf_training_size = int(training_len/16)
results = np.zeros(len(testing_data))
for i in range(0, 16):
	clf = RandomForestClassifier(n_estimators=8, max_depth=32, min_samples_split=2, criterion = "entropy")
	X = training_data[i * clf_training_size: (i + 1) * clf_training_size, 1:]
	Y = training_data[i * clf_training_size: (i + 1) * clf_training_size, 0]
	clf.fit(X, Y)
	result = clf.predict(testing_data[:, 1:])
	result = np.array(result)
	results = np.add(results, result)

for i in range(0, len(testing_data)):
	if results[i] >= 8:
		results[i] = 1
	else:
		results[i] = 0	
'''
#random forest parameter tuning
'''
bestAccuracy = 0
bestNEst = 0
bestDepth = 0

for i in range(4, 65, 4):
	for j in range(16, 129, 4):
		clf = RandomForestClassifier(n_estimators=i, max_depth=j, min_samples_split=2\
									,criterion = "entropy")
		X = training_data[:, 1:]
		Y = training_data[:, 0]
		clf.fit(X, Y)
		results = clf.predict(testing_data[:, 1:])
		counter = 0
		for k in range(0, len(testing_data)):
			if testing_data[k, 0] == results[k]:
				counter += 1
		accuracy = counter/len(testing_data)
		if accuracy > bestAccuracy:
			bestNEst = i
			bestDepth = j
			bestAccuracy = accuracy
print("best parameter is, number of estimator: ", bestNEst, "max depth: ", bestDepth, "Accuracy: ", bestAccuracy)
'''
print("decision tree")
for i in range(40, 201, 40):
	print("max depth is", i)
	clf = tree.DecisionTreeClassifier(max_depth = i)
	X = training_data[:, 1:]
	Y = training_data[:, 0]
	clf.fit(X, Y)
	results = clf.predict(testing_data[:, 1:])
	p = clf.predict_proba(testing_data[:, 1:])
	accuracy(p, results, testing_data)
	score()


print("random forest")
for i in range(30, 150, 30):
	for j in range(18, 100, 18):
		print(str(i) + " " + str(j))
		clf = RandomForestClassifier(n_estimators=i, max_depth=j, min_samples_split=2\
							,criterion = "entropy")
		X = training_data[:, 1:]
		Y = training_data[:, 0]
		clf.fit(X, Y)
		results = clf.predict(testing_data[:, 1:])
		p = clf.predict_proba(testing_data[:, 1:])
		accuracy(p, results, testing_data)
		score()



print("GradientBoostingClassifier")
print("100 iterations")
clf = GradientBoostingClassifier(n_estimators=100, learning_rate=1.0, max_depth=1, random_state=0)
X = training_data[:, 1:]
Y = training_data[:, 0]
clf.fit(X, Y)
results = clf.predict(testing_data[:, 1:])
p = clf.predict_proba(testing_data[:, 1:])
accuracy(p, results, testing_data)
score()

print("500 iterations")
arr = [1, .1, .01, .001, .0001]
for i in range (1, 5, 1):
	for j in arr:
		print(str(i) + " " + str(j))
		clf = GradientBoostingClassifier(n_estimators=500, learning_rate=j, max_depth=i, random_state=0)
		X = training_data[:, 1:]
		Y = training_data[:, 0]
		clf.fit(X, Y)
		results = clf.predict(testing_data[:, 1:])
		p = clf.predict_proba(testing_data[:, 1:])
		accuracy(p, results, testing_data)
		score()
print("1000 iterations")
clf = GradientBoostingClassifier(n_estimators=1000, learning_rate=1.0, max_depth=1, random_state=0)
X = training_data[:, 1:]
Y = training_data[:, 0]
clf.fit(X, Y)
results = clf.predict(testing_data[:, 1:])
p = clf.predict_proba(testing_data[:, 1:])
accuracy(p, results, testing_data)
score()


print("LogisticRegression")
clf = LogisticRegression()
X = training_data[:, 1:]
Y = training_data[:, 0]
clf.fit(X, Y)
results = clf.predict(testing_data[:, 1:])
p = clf.predict_proba(testing_data[:, 1:])
accuracy(p, results, testing_data)
score()

'''
model = SelectFromModel(clf, prefit = True)
X = model.transform(X)
testing_X = model.transform()
clf = RandomForestClassifier(n_estimators=60, max_depth=36, min_samples_split=2\
							,criterion = "entropy")
clf.fit(X, Y)	
'''						



