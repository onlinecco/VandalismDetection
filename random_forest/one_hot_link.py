
#get unique items

# f = open('out.csv')
# contentType_dic = {}
# revision_sub_dic = {}
# revisionAction_dic = {}
# revisionPrevAction = {}
# revisionTag = {}
# first = True
# for line in f:
# 	line = line.strip().split(',')
# 	if first:
# 		first = False
# 		continue

# 	contentType_dic[line[4]] = 1
# 	revisionAction_dic[line[62]] = 1
# 	revision_sub_dic[line[64]] = 1
# 	revisionPrevAction[line[63]] = 1
# 	revisionTag[line[65]] = 1

# out.write('contentType\n')
# for key in contentType_dic:
# 	out.write('\t'+key+'\n')
# out.write('\tcontentTypeEmpty\n')
# out.write('\tcontentTypeUnknown\n')

# out.write('revision_subaction\n')
# for key in revision_sub_dic:
# 	out.write('\t'+key+'\n')
# out.write('\trevision_subEmpty\n')
# out.write('\trevision_subUnknown\n')

# out.write('revisionPrevAction\n')
# for key in revisionPrevAction:
# 	out.write('\t'+key+'\n')
# out.write('\trevisionPrevActionEmpty\n')
# out.write('\trevisionPrevActionUnknown\n')

# out.write('revisionAction\n')
# for key in revisionAction_dic:
# 	out.write('\t'+key+'\n')
# out.write('\trevisionActionEmpty\n')
# out.write('\trevisionActionUnknown\n')

# out.write('revisionTag\n')
# for key in revisionTag:
# 	out.write('\t'+key+'\n')
# out.write('\trevisionTagEmpty\n')
# out.write('\trevisionTagUnknown\n')

# out.close()
# f.close()


#select_data
# f = open('out.csv')
# true_output = []
# false_output = []
# first_line = None
# for line in f:
# 	if not first_line:
# 		first_line = line
# 		continue
# 	if int(line.strip()[-1])>0:
# 		true_output.append(line)
# 	else:
# 		if len(false_output)<len(true_output)+50:
# 			false_output.append(line)

# print len(true_outcput)

# out = open('selected_data.csv','w')
# out.write(first_line)
# for line in true_output:
# 	out.write(line)
# for line in false_output:
# 	out.write(line)
# out.close()
# f.close()



#drop the revisionlang
# import pandas as pd
# df = pd.read_csv('./selected_data.csv')
# df.drop('revisionLanguage',1,inplace=True)
# df.to_csv('selected_data.csv',index = False)

#one hot link
f = open('keys')
key_dic = {}
for line in f:
	if not '\t' in line:
		curkey = line.strip()
		key_dic[curkey] = []
	else:
		key_dic[curkey].append(line.strip())


data = open('selected_data.csv')
datalist =[line.strip().split(',') for line in data]



contentType = key_dic['contentType']
for val in contentType:
	datalist[0].append(val)
for i in range(1,len(datalist)):
	find = False
	for val in contentType:
		val = val[len('contentType'):]
		if datalist[i][3] == val:
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
		if datalist[i][63] == val:
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
		if datalist[i][62] == val:
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
		if datalist[i][61] == val:
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
		if datalist[i][64] == val:
			find = True
			datalist[i].append('1')
		elif val == 'Empty' and not find:
			datalist[i].append('1')
		else:
			datalist[i].append('0')

outputlist = [','.join(datalist[i])+'\n' for i in range(len(datalist))]
out = open('selected_out.csv','w')
for line in outputlist:
	out.write(line)
f.close()
out.close()


import pandas as pd
df = pd.read_csv('./selected_out.csv')
df.drop('contentType',1,inplace=True)
df.drop('revisionTag',1,inplace=True)
df.drop('revisionAction',1,inplace=True)
df.drop('revisionPrevAction',1,inplace=True)
df.drop('revisionSubaction',1,inplace=True)
df.insert(0,'rollbackReverted_val', df['rollbackReverted'])
df.drop('rollbackReverted',1,inplace=True)
df.to_csv('selected_out.csv',index = False)
