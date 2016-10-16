with open('wdvc16_meta.csv') as f:
	operation = {}
	line = f.readline()
	i = 0
	while len(line)!= 0:
		# print line.strip().split(',') 
		if len(line.strip().split(",")[8])>0:
		   op = line.strip().split(",")[8]
		   operation[op] = operation.get(op,0)+1
		line = f.readline()
		# i+=1
		# if not i%10000000:
		# 	print i
		# 	print operation
		# 	print len(operation)

	print operation
	print len(operation)

