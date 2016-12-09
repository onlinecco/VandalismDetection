import os

#load datamodel here
import pandas as pd
import csv
import numpy as np
import random
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib


featureFile = '/home/barberry/data/feature'
tmpFile = './tmp'
outFile = './out'
keyFile = './key'

clf = joblib.load('tree.pkl')

f = open(keyFile)
key_dic = {}
for line in f:
    if not '\t' in line:
        curkey = line.strip()
        key_dic[curkey] = []
    else:
        key_dic[curkey].append(line.strip())
f.close()

def test():

	#STEP1 clean the attribute we do not need
	df = pd.read_csv(featureFile)

	revisionIdStr = df['revisionId']

	df.drop('revisionId',1,inplace=True)
	df.drop('userId',1,inplace=True)
	df.drop('userName',1,inplace=True)
	df.drop('timestamp',1,inplace=True)
	df.drop('revisionLanguage',1,inplace=True)
	df.drop('commentTail',1,inplace=True)
	df.drop('itemId',1,inplace=True)
	df.drop('englishItemLabel',1,inplace=True)
	df.drop('superItemId',1,inplace=True)
	df.drop('latestInstanceOfItemId',1,inplace=True)
	df.drop('latestEnglishItemLabel',1,inplace=True)
	df.drop('property',1,inplace=True)
	df.drop('userCity',1,inplace=True)
	df.drop('userContinent',1,inplace=True)
	df.drop('userCountry',1,inplace=True)
	df.drop('userCounty',1,inplace=True)
	df.drop('userRegion',1,inplace=True)
	df.drop('userTimeZone',1,inplace=True)
	df.drop('literalValue',1,inplace=True)
	df.drop('param1',1,inplace=True)
	df.drop('param3',1,inplace=True)
	df.drop('param4',1,inplace=True)
	df.drop('timeSinceLastRevision',1,inplace=True)
	df.drop('undoRestoreReverted',1,inplace=True)
	df.drop('parentRevisionInCorpus',1,inplace= True)


	#STEP2 map the true false to 0 and 1
	df['containsLanguageWord'] = df['containsLanguageWord'].map({'T':1,'F':0})
	df['containsURL'] = df['containsURL'].map({'T':1,'F':0})
	df['isPrivilegedUser'] = df['isPrivilegedUser'].map({'T':1,'F':0})
	df['isBotUser'] = df['isBotUser'].map({'T':1,'F':0})
	df['isRegisteredUser'] = df['isRegisteredUser'].map({'T':1,'F':0})
	df['hasListLabel'] = df['hasListLabel'].map({'T':1,'F':0})
	df['isHuman'] = df['isHuman'].map({'T':1,'F':0})
	df['labelContainsFemaleFirstName'] = df['labelContainsFemaleFirstName'].map({'T':1,'F':0})
	df['labelContainsMaleFirstName'] = df['labelContainsMaleFirstName'].map({'T':1,'F':0})
	df['minorRevision'] = df['minorRevision'].map({'T':1,'F':0})
	df['hasP21Changed'] = df['hasP21Changed'].map({'T':1,'F':0})
	df['hasP27Changed'] = df['hasP27Changed'].map({'T':1,'F':0})
	df['hasP54Changed'] = df['hasP54Changed'].map({'T':1,'F':0})
	df['hasP18Changed'] = df['hasP18Changed'].map({'T':1,'F':0})
	df['hasP569Changed'] = df['hasP569Changed'].map({'T':1,'F':0})
	df['hasP109Changed'] = df['hasP109Changed'].map({'T':1,'F':0})
	df['hasP373Changed'] = df['hasP373Changed'].map({'T':1,'F':0})
	df['hasP856Changed'] = df['hasP856Changed'].map({'T':1,'F':0})
	df['rollbackReverted'] = df['rollbackReverted'].map({'T':1,'F':0})
	df['isLivingPerson'] = df['isLivingPerson'].map({'T':1,'F':0})
	df['englishLabelTouched'] = df['englishLabelTouched'].map({'T':1,'F':0})
	df['isLatinLanguage'] = df['isLatinLanguage'].map({'T':1,'F':0})
	df.to_csv(tmpFile,index = False)

	#STEP5 One hot link
	out = open(outFile,'w')
	true_output = []
	false_output = []
	f = open(tmpFile)
	header = f.readline()
	for line in f:
	    if int(line.strip()[-1])==1:
	        true_output.append(line)
	    else:
	        if len(false_output)<len(true_output)+50:
	            false_output.append(line)
	f.close()

	out.write(header)
	for line in true_output:
	    out.write(line)
	for line in false_output:
	    out.write(line)
	out.close()




	data = open(outFile)

	datalist = [line.strip().split(',') for line in data]

	contentType = key_dic['contentType']
	for val in contentType:
		datalist[0].append(val)
	for i in range(1,len(datalist)):
		find = False
		for val in contentType:
			val = val[len('contentType'):]
			if datalist[i][1] == val:
				datalist[i].append('1')
			elif val == 'Empty' and datalist[i][1]=='':
				datalist[i].append('1')
			else:
				datalist[i].append('0')

	revisionAction = key_dic['revisionAction']
	for val in revisionAction:
		datalist[0].append(val)
	for i in range(1,len(datalist)):
		find = False
		for val in revisionAction:
			val = val[len('revisionAction'):]
			if datalist[i][57] == val:
				datalist[i].append('1')
			elif val == 'Empty' and datalist[i][57]=='':
				datalist[i].append('1')
			else:
				datalist[i].append('0')

	revisionPrevAction = key_dic['revisionPrevAction']
	for val in revisionPrevAction:
		datalist[0].append(val)
	for i in range(1,len(datalist)):
		find = False
		for val in revisionPrevAction:
			val = val[len('revisionPrevAction'):]
			if datalist[i][58] == val:
				datalist[i].append('1')
			elif val == 'Empty' and datalist[i][58]=='':
				datalist[i].append('1')
			else:
				datalist[i].append('0')


	revisionSubaction = key_dic['revisionSubaction']
	for val in revisionSubaction:
		datalist[0].append(val)
	for i in range(1,len(datalist)):
		find = False
		for val in revisionSubaction:
			val = val[len('revisionSubaction'):]
			if datalist[i][59] == val:
				datalist[i].append('1')
			elif val == 'Empty' and datalist[i][59]=='':
				datalist[i].append('1')
			else:
				datalist[i].append('0')

	revisionTag = key_dic['revisionTag']
	for val in revisionTag:
		datalist[0].append(val)
	for i in range(1,len(datalist)):
		find = False
		for val in revisionTag:
			val = val[len('revisionTag'):]

			if datalist[i][60] == val or (datalist[i][60] and datalist[i][60][1:] == val):
				datalist[i].append('1')
			elif val == 'Empty' and datalist[i][60]=='':
				datalist[i].append('1')
			else:
				datalist[i].append('0')


	outputlist = [','.join(datalist[i])+'\n' for i in range(len(datalist))]
	out = open(outFile,'w')

	for line in outputlist:
		out.write(line)
	# f.close()
	out.close()


	#Step6 final prep, delete unused attribute
	df = pd.read_csv(outFile)

	df.drop('contentType',1,inplace=True)
	df.drop('revisionTag',1,inplace=True)
	df.drop('revisionAction',1,inplace=True)
	df.drop('revisionPrevAction',1,inplace=True)
	df.drop('revisionSubaction',1,inplace=True)
	df.drop('groupId',1,inplace=True)
	df.insert(0,'rollbackReverted_val', df['rollbackReverted'])
	df.drop('rollbackReverted',1,inplace=True)
	df.drop('itemValue',1,inplace=True)

	df.to_csv(tmpFile, index=False)


	f = open(tmpFile)
	out = open(outFile,'w')

	title = f.readline().strip()
	out.write(title+'\n')
	line = f.readline()
	while len(line) > 1:
		arr = line.strip().split(',')
		for i in range(len(arr)):
			try:
				float(arr[i])
			except:
				arr[i] = 0
			if i == 0:
				out.write(str(arr[i]))
			else:
				out.write(','+str(arr[i]))
		out.write('\n')
		line = f.readline()
	out.close()
	f.close()


	with open(outFile, 'rb') as h:
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

	    # print("start predict:")
	    pre = clf.predict_proba(x_test)
	    # print (pre[:, 1])

	transferFile = open('/home/barberry/data/result', 'w')
	for i in range(len(revisionIdStr)):
		transferFile.write(str(revisionIdStr[i])+' '+ str(pre[i,1])+'\n')
    transferFile.close()


a = open("/home/barberry/data/notify",'w')
a.write("datamodelloaded\n")
a.close()
while True:

	a = open("/home/barberry/data/notify",'r')
	b = a.read().strip()
	a.close()
	if b == "datahasarrived":

		#predict
		test()
		
		a = open("/home/barberry/data/notify",'w')
		a.write("classified\n")
		a.close()
