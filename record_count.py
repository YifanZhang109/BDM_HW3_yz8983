import csv
import pyspark

def reader(pid, lines):
    for row in csv.reader(lines):
        yield (len(row), 1)

sc = pyspark.SparkContext()
print(sc.textFile('/shared/CUSP-GX-6002/data/complaints.csv') \
  .mapPartitionsWithIndex(reader) \
  .reduceByKey(lambda x,y: x+y) \
  .collect())
