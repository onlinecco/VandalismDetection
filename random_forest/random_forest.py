from sklearn.ensemble import RandomForestClassifier
import pandas as pd
from numpy import genfromtxt, savetxt
"""
df = pd.read_csv('./FEATURES')
#print df.head(5)

#drop
df.drop('userName',1,inplace=True)
df.drop('timestamp',1,inplace=True)
df.drop('commentTail',1,inplace=True)
df.drop('englishItemLabel',1,inplace=True)
df.drop('latestEnglishItemLabel',1,inplace=True)
df.drop('property',1,inplace=True)
df.drop('userCity',1,inplace=True)
df.drop('userContinent',1,inplace=True)
df.drop('userCountry',1,inplace=True)
df.drop('userRegion',1,inplace=True)
df.drop('userTimeZone',1,inplace=True)
df.drop('param1',1,inplace=True)
df.drop('param3',1,inplace=True)
df.drop('param4',1,inplace=True)
df.drop('timeSinceLastRevision',1,inplace=True)

#eliminate NA
df.replace('NA',np.nan,inplace=True)
 
#map True False value
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
df['undoRestoreReverted'] = df['undoRestoreReverted'].map({'T':1,'F':0})
df['rollbackReverted'] = df['rollbackReverted'] + df['undoRestoreReverted']
df.drop('undoRestoreReverted',1,inplace=True)

df.to_csv('out.csv',index = False)
"""
# df = open('./out.csv')
# print 'finished reading'
# line = df.readline()
# w = open('out2.csv','w')
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

# df = pd.read_csv('./out2.csv')
# df['isLivingPerson'] = df['isLivingPerson'].map({'T':1,'F':0})
# df['englishLabelTouched'] = df['englishLabelTouched'].map({'T':1,'F':0})
# df.drop('userCounty',1,inplace=True)

#df = pd.read_csv('./out.csv')
#df['isLatinLanguage'] = df['isLatinLanguage'].map({'T':1,'F':0})
#df.drop('literalValue',1,inplace=True)
#df.to_csv('out.csv',index = False)

#list_feature = ['revisionLanguage','contentType','revisionSubaction','revisionAction','revisionPrevAction','revisionTag']
# list_feature = ['revisionTag']
# df = pd.read_csv('./out.csv')
# for feature in list_feature:
# 	A = df[feature]
# 	temp = pd.get_dummies(A)
# 	df = pd.concat([df,temp],axis=1,join_axes=[df.index])
# 	df.drop(feature,1,inplace=True)
	
# df.to_csv('out1.csv',index = False)
# df = pd.read_csv('./out.csv')
# df.drop('contentType',1,inplace=True)
# df.drop('revisionAction',1,inplace=True)
# df.drop('revisionSubaction',1,inplace=True)
# df.drop('revisionPrevAction',1,inplace=True)
# #df.drop('revisionTag',1,inplace=True)
# list_feature = ['revisionTag']
# #df = pd.read_csv('./out.csv')
# for feature in list_feature:
# 	A = df[feature]
# 	temp = pd.get_dummies(A)
# 	df = pd.concat([df,temp],axis=1,join_axes=[df.index])
# 	df.drop(feature,1,inplace=True)
# df.fillna(0)
# df.insert(0,'rollbackReverted_val', df['rollbackReverted'])
# df.drop('rollbackReverted',1,inplace=True)
# df.to_csv('out1.csv',index= False)



#convert numeric feature to float
# num_of_col = len(df.columns)
# for i in xrange(num_of_col):
# 	val = df.get_value(1,i)
# 	try:
# 		float(val)
# 		df.columns[i] = df.columns[i].astype(float).fillna(0.0)
# 	except:
# 		continue

#df = pd.get_dummies(df,dummy_na=True)
#print df.head(2)

dataset = genfromtxt(open('test.csv','r'), delimiter=',', dtype='f8' ,filling_values='0')


#print len(line_1),len(line_2)
target = [x[0] for x in dataset]
train = [x[1:] for x in dataset]

rf = RandomForestClassifier(n_estimators=100)
rf.fit(train, target)

testset = genfromtxt(open('score.csv','r'), delimiter=',', dtype='f8' ,filling_values='0')

testval = [x[0] for x in testset]
testtrain = [x[1:] for x in testset]

score = rf.score(testtrain,testval)
print 'the score is ', score