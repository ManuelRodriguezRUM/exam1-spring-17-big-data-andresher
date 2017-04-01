#Code here

eFile = sc.textFile('/home/andres/p3exam1/escuelasPR.csv')
e = eFile.map(lambda x: x.split(','))
result = e.filter(lambda x: x[2]=='Ponce' or x[2]=='Cabo Rojo' or x[2]=='Dorado')
result.collect()