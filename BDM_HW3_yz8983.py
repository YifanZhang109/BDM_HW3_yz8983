from pyspark import SparkContext
import sys
import csv

sc = pyspark.SparkContext.getOrCreate()
COMPLAINTS_FN = 'complaints.csv'

def getline(pid, lines):
  for row in csv.reader(lines):
    if(len(row)==18):
      yield(row)

rdd = sc.textFile(COMPLAINTS_FN)
rddHeader = rdd.first().split(',')
rdd = rdd.filter(lambda x: not x.startswith('Date received,Product,'))\
    .mapPartitionsWithIndex(getline)

def get_statistics(companies):
    number_complaints = len(companies)
    unique_companies = set(companies)
    num_companies = len(unique_companies)
    company_complaints = [(company, len([c for c in companies if c == company])) for company in unique_companies]
    company_complaints.sort(key=lambda x: x[1], reverse=True)
    highest_percentage = round(company_complaints[0][1] / number_complaints * 100)
    return (number_complaints, num_companies, highest_percentage)

outputTask1 = rdd.map(lambda x: (x[0].split('-')[0],x[1].lower(),x[7].lower()))\
      .groupBy(lambda x: (x[0], x[1]))\
      .mapValues(lambda x: [y[2] for y in x])\
      .mapValues(get_statistics)\
      .sortBy(lambda x: (x[0][1], x[0][0]))\
      .map(lambda x: [str(x[0][1]),str(x[0][0]),str(x[1][0]),str(x[1][1]),str(x[1][2])])\
      .map(lambda x: ','.join(x))

outputTask1.saveAsTextFile('output')
