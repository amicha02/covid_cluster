import pyspark
import pandas as pd 
import ast,datetime
import csv
import sys
import statistics
from pyspark.sql import SQLContext
from pyspark import sql
if __name__=='__main__':
    def categorize(x):
      if x == '452210' or x == '452311':
        yield ('big_box_grocers')
      elif x == '445120':
        yield ('convenience_stores')
      elif x == '722410':
        yield ('drinking_places')
      elif x == '722511':
        yield  ('full_service_restaurants')
      elif x == '722513':
        yield ('limited_service_restaurants')
      elif x == '446110' or x == '446191':
        yield ('pharmacies_and_drug_stores')
      elif x == '311811' or x == '722515':
        yield ('snack_and_bakeries')
      elif x in ['445210', '445220', '445230', '445291', '445292','445299']:
        yield ('specialty_food_stores')
      elif x == '445110':
        yield ('supermarkets_except_convenience_stores')
      else:
        yield('whatever')
    def extractPlaces(partId, records):
        if partId==0:
            next(records)
        reader = csv.reader(records)
        for row in reader:
            (id,category) = (row[1],next(categorize(row[9])))
            if category != 'whatever':
                yield (id,category)
    def range_f(start,end):
        start = datetime.datetime.strptime(start.split('T')[0] , '%Y-%m-%d')
        end = datetime.datetime.strptime(end.split('T')[0] , '%Y-%m-%d')
        range = pd.date_range(start, end)
        result = [date.strftime('%Y-%m-%d') for date in range]
        yield result[:-1]
    def extractSchools(partId, list_of_records):
        if partId==0: 
            next(list_of_records) # skipping the first line
        reader = csv.reader(list_of_records)
        for row in reader:
                (id, dates,visits) = (row[1], next(range_f(row[12], row[13])),row[16])
                for i,date in enumerate(dates):
                  yield (id, (date,ast.literal_eval(visits)[i]))
    def zeroed(number):
        if number >= 0:
            yield (number)
        if number <0:
            yield (0)
    sc = pyspark.SparkContext()
    sqlContext = sql.SQLContext(sc)
    places = sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv', use_unicode=False).cache()
    placesrdd= places.mapPartitionsWithIndex(extractPlaces)
    patterns = sc.textFile('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/part-00000', use_unicode=False).cache()
    rdd = patterns.mapPartitionsWithIndex(extractSchools).join(placesrdd).values().map(lambda x: (x[0][0],x[0][1],x[1])).sortBy(lambda x: x[0])
    categories = ['big_box_grocers','convenience_stores','drinking_places','full_service_restaurants','limited_service_restaurants','pharmacies_and_drug_stores','snack_and_bakeries','specialty_food_stores','supermarkets_except_convenience_stores']
    for category in categories:
        rdd1 = rdd.filter(lambda x: x[2]== category).map(lambda x: (x[0],x[1])).groupByKey().mapValues(statistics.median)
        rdd2 = rdd.filter(lambda x: x[2]== category).map(lambda x: (x[0],x[1])).groupByKey().mapValues(statistics.stdev)
        rdd2.join(rdd1).map(lambda x: (x[0].split('-')[0],x[0],x[1][0],x[1][1],next(zeroed(x[1][0]-x[1][1])),x[1][0]+x[1][1])).saveAsTextFile('test/'+ category)
