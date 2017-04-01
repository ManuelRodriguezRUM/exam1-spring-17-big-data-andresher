#Code here

sFile = sc.textFile('/home/andres/p3exam1/studentsPR.csv')
s = sFile.map(lambda x: x.split(','))
result = s.filter(lambda x: x[5]=='F').filter(lambda x: x[2]=='71381')
result.collect()