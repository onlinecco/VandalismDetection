# generate tags file with revisionID, sha1, null, tags

with open('out.csv') as f:
	with open('wdvc16_meta.csv') as f1:
		dic = {}
		line = f.readline()
		while len(line)!= 0:
			line = line.strip().split(',')
			dic[line[0]] = []
			dic[line[0]].append(line[1])
			dic[line[0]].append(line[2])
			line = f.readline()

		line_meta = f1.readline()
		line_meta = f1.readline()

		print line_meta
		while len(line_meta) != 0:
			#print line_meta
			line_meta = line_meta.strip().split(',')
			revision_id = line_meta[0] 
			if int(revision_id)>8303205:
				break
			revision_id = '"'+revision_id+'"'
			if revision_id in dic:
				revision_tag = line_meta[8]
				if not revision_tag:
					line_meta = f1.readline()
					continue

				if revision_tag[0] == '"':
					revision_tag+='"'
				else:
					revision_tag = '"' + revision_tag + '"'
				if len(revision_tag)>28:
					revision_tag[len(revision_tag)-28:]

				dic[revision_id].append(revision_tag)
				print revision_id, dic[revision_id]

			line_meta = f1.readline()
		f1.close()

	with open('output.csv','w') as f2:
		for key in sorted(dic):
			output = ','.join(dic[key])
			output = key+','+output+'\n'
			f2.write(output)
		f2.close()

	f.close()

			#if len(line_meta.strip().split(',')[8]) > 0


