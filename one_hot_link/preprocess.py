import statistics

with open('./test1.csv') as f:
    outputFile = open('./testoutput.csv', 'w')
    matrix = []
    line = f.readline()
    outputFile.write(line.strip()+'\n')
    line = f.readline()
    arr = []
    while len(line) != 0:
        arr = line.strip().split(',')
        matrix.append(arr)
        line = f.readline()
    print('arr = ' + str(len(arr)) + '  matrix = ' + str(len(matrix)) )
    for i in range(len(arr)):
        print(str(i) + ' ' + str(len(arr)))
        col = []
        if i != 34 and i != 37:
            for j in range(len(matrix)):
                tmp = matrix[j][i]
                # if i == 37:
                #     print(tmp)
                if len(tmp) > 0:
                    if '.' in tmp:
                        col.append(float(tmp))
                    else:
                        col.append(int(tmp))
            if len(col) == 0:
                print(i)
            med = statistics.median(col)
            for j in range(len(matrix)):
                tmp = matrix[j][i]
                if len(tmp) == 0:
                    matrix[j][i] = str(med)
    for i in range(len(matrix)):
        print(str(i) + ' ' + str(len(matrix)))
        for j in range(len(matrix[i])):
            if j == 0:
                outputFile.write(matrix[i][j])
            else:
                outputFile.write(','+matrix[i][j])
        outputFile.write('\n')
    outputFile.close()

