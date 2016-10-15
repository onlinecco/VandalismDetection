with open('wdvc16_meta.csv') as f:
    line = f.readline()
    while len(line)!= 0:
        if "2916961" == line.split(",")[0] or "2916994" == line.split(",")[3]:
            print line
        line = f.readline()
