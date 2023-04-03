from pyspark import SparkContext
import sys
import csv

if __name__=='__main__':
    
    def getline(pid, lines):
      for row in csv.reader(lines):
        if(len(row)==18):
          yield(row)

    sc = pyspark.SparkContext.getOrCreate()
    
    rdd = sc.textFile(sys.argv[1] if len(sys.argv)>1 else '/shared/CUSP-GX-6002/data/complaints.csv')
    rdd = rdd.filter(lambda x: not x.startswith('Date received,Product,'))\
        .mapPartitionsWithIndex(getline)

    outputTask1 = rdd.map(lambda x: (x[0].split('-')[0],x[1].lower(),x[7].lower()))\
          .groupBy(lambda x: (x[0], x[1]))\
          .mapValues(lambda x: [y[2] for y in x])\
          .mapValues(get_statistics)\
          .sortBy(lambda x: (x[0][1], x[0][0]))\
          .map(lambda x: [str(x[0][1]),str(x[0][0]),str(x[1][0]),str(x[1][1]),str(x[1][2])])\
          .map(lambda x: ','.join(x))
    
    outputTask1.saveAsTextFile(sys.argv[2] if len(sys.argv)>2 else 'output')
