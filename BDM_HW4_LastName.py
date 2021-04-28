import pyspark
from pyspark.sql import SQLContext
from pyspark import sql
import pandas as pd 
from pyspark.sql.functions import arrays_zip, col, explode
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
    def range_f(start,end):
      list1 =[]
      start = datetime.datetime.strptime(start.split('T')[0] , '%Y-%m-%d')
      end = datetime.datetime.strptime(end.split('T')[0] , '%Y-%m-%d')
      range = pd.date_range(start, end)
      for date in range:
        list1.append(date.strftime('%Y-%m-%d'))
      yield list1[:-1]
    sc = pyspark.SparkContext()
    sqlContext = sql.SQLContext(sc)
    places = sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv').map(lambda x: x.split(',')).map(lambda x: (next(categorize(x[9])),x[1])).filter(lambda x: x[0] != 'whatever').collect()
    rdd_places =sc.parallelize(places)
    df_places = rdd_places.toDF(['category','store_id'])
    df = sc.textFile('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/part-00000') \
        .map(lambda x: next(csv.reader([x.encode('utf-8')]))) \
        .filter(lambda x: len(x[12]) == 25) \
        .map(lambda x: (x[1],next(range_f(x[12],x[13])), ast.literal_eval(x[16]))) \
        .collect()
    rdd =sc.parallelize(df)
    df_pattern = rdd.toDF(['store_id','dates','visits'])
    df1 = df_pattern.join(df_places, df_pattern.store_id == df_places.store_id,'inner')
    df1= (df1
        .withColumn("tmp", arrays_zip("dates", "visits"))
        .withColumn("tmp", explode("tmp"))
        .select('category',col("tmp.dates"), col("tmp.visits")))
    categories = ['big_box_grocers','convenience_stores','drinking_places','full_service_restaurants','limited_service_restaurants','pharmacies_and_drug_stores','snack_and_bakeries','specialty_food_stores','supermarkets_except_convenience_stores']
    for category in categories:
      df_cat = df1.filter(F.col('category') == category)
      magic_percentile = F.expr('percentile_approx(visits, 0.5)')
      magic_std = F.expr('stddev(visits)')
      df_new = df_cat.groupBy('dates').agg(magic_percentile.alias('median'),magic_std.alias('std')).withColumn('year', pyspark.sql.functions.split(df1['dates'], '-').getItem(0))
      df1_new = df_new.withColumn('low', ( df_new['median'] - df_new['std'] )  ).withColumn('high', ( df_new['median'] + df_new['std'] )  ).withColumn("low", F.when(F.col("low") > 0, F.col("low")).otherwise(0)).orderBy('dates')
      df_new = df1_new.select('year','dates','median','low','high')
      rdd = df_new.rdd.map(tuple)
      rdd.saveAsTextFile(category)