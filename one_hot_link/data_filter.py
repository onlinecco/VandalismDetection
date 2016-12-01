import pandas as pd
# df = pd.read_csv('./features/FEATURES_2016_01')
# df.drop('revisionId',1,inplace=True)
# df.drop('userId',1,inplace=True)
# df.drop('userName',1,inplace=True)
# df.drop('timestamp',1,inplace=True)
# df.drop('revisionLanguage',1,inplace=True)
# df.drop('commentTail',1,inplace=True)
# df.drop('itemId',1,inplace=True)
# df.drop('englishItemLabel',1,inplace=True)
# df.drop('superItemId',1,inplace=True)
# df.drop('latestInstanceOfItemId',1,inplace=True)
# df.drop('latestInstanceOfItemId',1,inplace=True)
# df.drop('latestEnglishItemLabel',1,inplace=True)
# df.drop('property',1,inplace=True)
# df.drop('groupId',1,inplace=True)
# df.drop('userCity',1,inplace=True)
# df.drop('userContinent',1,inplace=True)
# df.drop('userCountry',1,inplace=True)
# df.drop('userRegion',1,inplace=True)
# df.drop('userTimeZone',1,inplace=True)
# df.drop('literalValue',1,inplace=True)
# df.drop('param1',1,inplace=True)
# df.drop('param3',1,inplace=True)
# df.drop('param4',1,inplace=True)
# df.drop('timeSinceLastRevision',1,inplace=True)
# df.drop('undoRestoreReverted',1,inplace=True)
# df.drop('parentRevisionInCorpus',1,inplace= True)
# df['containsLanguageWord'] = df['containsLanguageWord'].map({'T':1,'F':0})
# df['containsURL'] = df['containsURL'].map({'T':1,'F':0})
# df['isPrivilegedUser'] = df['isPrivilegedUser'].map({'T':1,'F':0})
# df['isBotUser'] = df['isBotUser'].map({'T':1,'F':0})
# df['isRegisteredUser'] = df['isRegisteredUser'].map({'T':1,'F':0})
# df['hasListLabel'] = df['hasListLabel'].map({'T':1,'F':0})
# df['isHuman'] = df['isHuman'].map({'T':1,'F':0})
# df['labelContainsFemaleFirstName'] = df['labelContainsFemaleFirstName'].map({'T':1,'F':0})
# df['labelContainsMaleFirstName'] = df['labelContainsMaleFirstName'].map({'T':1,'F':0})
# df['minorRevision'] = df['minorRevision'].map({'T':1,'F':0})
# df['hasP21Changed'] = df['hasP21Changed'].map({'T':1,'F':0})
# df['hasP27Changed'] = df['hasP27Changed'].map({'T':1,'F':0})
# df['hasP54Changed'] = df['hasP54Changed'].map({'T':1,'F':0})
# df['hasP18Changed'] = df['hasP18Changed'].map({'T':1,'F':0})
# df['hasP569Changed'] = df['hasP569Changed'].map({'T':1,'F':0})
# df['hasP109Changed'] = df['hasP109Changed'].map({'T':1,'F':0})
# df['hasP373Changed'] = df['hasP373Changed'].map({'T':1,'F':0})
# df['hasP856Changed'] = df['hasP856Changed'].map({'T':1,'F':0})
# df['rollbackReverted'] = df['rollbackReverted'].map({'T':1,'F':0})
# df['isLivingPerson'] = df['isLivingPerson'].map({'T':1,'F':0})
# df['englishLabelTouched'] = df['englishLabelTouched'].map({'T':1,'F':0})
# df['isLatinLanguage'] = df['isLatinLanguage'].map({'T':1,'F':0})
# df.to_csv('./features/FEATURES_2016_01_1.csv',index = False)

# df = open('./features/FEATURES_2016_01_1.csv')
# print 'finished reading'
# line = df.readline()

# w = open('./features/FEATURES_2016_01_2.csv','w')
# w.write(line)
# while True:
# 	line = df.readline()
# 	if line == "":
# 		break
# 	c = line.strip().split(",")
# 	flag = True
# 	for token in c:
# 		try:
# 			float(token)
# 		except ValueError:
# 			try:
# 				if isinstance(token, str) and len(token) ==3:
# 					token[0].decode('ascii')
# 			except UnicodeDecodeError:
# 				flag = False
# 				break
# 	if flag:
# 		w.write(line)
# w.close()
# df.close()

# print "one hot link"
# filelist = ['FEATURES_2015_07_2.csv','FEATURES_2015_05_2.csv','FEATURES_2015_01_2.csv','FEATURES_2015_03_2.csv','FEATURES_2015_11_2.csv','FEATURES_2016_01_2.csv']

# #1 contentType
# #57 revisionAction
# #58 revisionPrevAction
# #59 revisionSubaction
# #60 revisionTag
# contentType = {}
# revisionAction = {}
# revisionPrevAction = {}
# revisionSubaction = {}
# revisionTag = {}
# for f in filelist:
# 	f = open('./features/'+f)
# 	f.readline()
# 	for line in f:
# 		line = line.strip().split(',')
# 		contentType[line[1]] = 1
# 		revisionAction[line[57]] = 1
# 		revisionPrevAction[line[58]] = 1
# 		revisionSubaction[line[59]] = 1
# 		#escape " in the first char
# 		if line[60] and line[60][0] == '"':
# 			revisionTag[line[60][1:]] = 1
# 		else:
# 			revisionTag[line[60]] = 1
# 	f.close()

# out = open('./features/key','w')
# out.write('contentType\n')
# for key in contentType:
# 	if key == "":
# 		continue
# 	out.write('\tcontentType'+key+'\n')
# out.write('\tcontentTypeEmpty\n')

# out.write('revisionAction\n')
# for key in revisionAction:
# 	if key == "":
# 		continue
# 	out.write('\trevisionAction'+key+'\n')
# out.write('\trevisionActionEmpty\n')


# out.write('revisionPrevAction\n')
# for key in revisionPrevAction:
# 	if key == "":
# 		continue
# 	out.write('\trevisionPrevAction'+key+'\n')
# out.write('\trevisionPrevActionEmpty\n')


# out.write('revisionSubaction\n')
# for key in revisionSubaction:
# 	if key == "":
# 		continue
# 	out.write('\trevisionSubaction'+key+'\n')
# out.write('\trevisionSubactionEmpty\n')

# out.write('revisionTag\n')
# for key in revisionTag:
# 	if key == "":
# 		continue
# 	out.write('\ttrevisionTag'+key+'\n')
# out.write('\trevisionTagEmpty\n')

# out.close()

# print 'select data'
# out = open('./features/selected_data.csv','w')
# true_output = []
# false_output = []
# filelist = ['FEATURES_2015_07_2.csv','FEATURES_2015_05_2.csv','FEATURES_2015_01_2.csv','FEATURES_2015_03_2.csv','FEATURES_2015_11_2.csv','FEATURES_2016_01_2.csv']
# for f in filelist:
# 	f = open('./features/'+f)
# 	header = f.readline()
# 	for line in f:
# 		if int(line.strip()[-1])==1:
# 			true_output.append(line)
# 		else:
# 			if len(false_output)<len(true_output)+50:
# 				false_output.append(line)
# 	f.close()

# out.write(header)
# for line in true_output:
# 	out.write(line)
# for line in false_output:
# 	out.write(line)
# out.close()



f = open('./features/key')
key_dic = {}
for line in f:
	if not '\t' in line:
		curkey = line.strip()
		key_dic[curkey] = []
	else:
		key_dic[curkey].append(line.strip())


data = open('./features/selected_data.csv')
datalist =[line.strip().split(',') for line in data]

contentType = key_dic['contentType']
for val in contentType:
	datalist[0].append(val)
for i in range(1,len(datalist)):
	find = False
	for val in contentType:
		val = val[len('contentType'):]
		if datalist[i][1] == val:
			find = True
			datalist[i].append('1')
		elif val == 'Empty' and not find:
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
			find = True
			datalist[i].append('1')
		elif val == 'Empty' and not find:
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
			find = True
			datalist[i].append('1')
		elif val == 'Empty' and not find:
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
			find = True
			datalist[i].append('1')
		elif val == 'Empty' and not find:
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
			find = True
			datalist[i].append('1')
		elif val == 'Empty' and not find:
			datalist[i].append('1')
		else:
			datalist[i].append('0')

outputlist = [','.join(datalist[i])+'\n' for i in range(len(datalist))]
out = open('./features/selected_out.csv','w')
for line in outputlist:
	out.write(line)
f.close()
out.close()

df = pd.read_csv('./features/selected_out.csv')
df.drop('contentType',1,inplace=True)
df.drop('revisionTag',1,inplace=True)
df.drop('revisionAction',1,inplace=True)
df.drop('revisionPrevAction',1,inplace=True)
df.drop('revisionSubaction',1,inplace=True)
df.insert(0,'rollbackReverted_val', df['rollbackReverted'])
df.drop('rollbackReverted',1,inplace=True)
df.to_csv('./features/selected_out.csv',index = False)
