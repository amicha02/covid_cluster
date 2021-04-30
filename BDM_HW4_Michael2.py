import pyspark
from pyspark.sql import SQLContext
from pyspark import sql
import pandas as pd 
from pyspark.sql.functions import col
import ast,datetime
import pyspark.sql.functions as F
import csv
import sys
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
        import csv
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
    sc = pyspark.SparkContext()
    sqlContext = sql.SQLContext(sc)
    places = sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv', use_unicode=True).cache()
    placesrdd= places.mapPartitionsWithIndex(extractPlaces)
    patterns = sc.textFile('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/part-00000', use_unicode=True).cache()
    rdd = patterns.mapPartitionsWithIndex(extractSchools).join(placesrdd).values().map(lambda x: (x[0][0],x[0][1],x[1])).sortBy(lambda x: x[0])
    categories = ['big_box_grocers','convenience_stores','drinking_places','full_service_restaurants','limited_service_restaurants','pharmacies_and_drug_stores','snack_and_bakeries','specialty_food_stores','supermarkets_except_convenience_stores']
    df = rdd.toDF(['date','visits','category'])
    for category in categories:
          df_category = df.filter(F.col('category') == 'full_service_restaurants')
          magic_percentile = F.expr('percentile_approx(visits, 0.5)')
          magic_std = F.expr('stddev(visits)')
          split_col = pyspark.sql.functions.split(df_category['date'], '-')
          df_new = df_category.groupBy('date').agg(magic_percentile.alias('median'),magic_std.alias('std'))
          df1_new = df_new.withColumn('year', split_col.getItem(0)).withColumn('low', ( df_new['median'] - df_new['std'] ) ).withColumn('high', ( df_new['median'] + df_new['std'] )  ).withColumn("low", F.when(F.col("low") > 0, F.col("low")).otherwise(0))
          df1_new.write.csv(category)