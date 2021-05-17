from pyspark import SparkContext
import datetime
import csv
import functools
import json
import numpy as np
import sys
from datetime import timedelta
import dateutil.parser
from dateutil.relativedelta import relativedelta
def main(sc):
    #filter the Core Places data set to extract the store IDs of interest
    def filterPOIs(CAT_CODES,CAT_GROUP,_, lines):
        reader = csv.reader(lines)
        for row in reader:
            if row[9] in CAT_CODES:
                yield (row[0],CAT_GROUP[row[9]])
    #filter our pattern data with storeGroup by mapping the placekey to the group number
    #  0: placekey
    # 12: date_range_start
    # 14: raw_visit_counts
    # 16: visits_by_day
    def extractVisits(storeGroup, _, lines):
        reader = csv.reader(lines)
        for row in reader:
          if row[0] in storeGroup: 
              for index,value in enumerate(json.loads(row[16])): 
                    if (dateutil.parser.parse(row[12])+ datetime.timedelta(days=index)).year in [2019,2020]:
                        yield ((storeGroup[row[0]],(dateutil.parser.parse(row[12]).replace(tzinfo=None)+ relativedelta(day=index)-dateutil.parser.parse('2019-01-01').replace(tzinfo=None)).days),value)
               
    def computeStats(groupCount, _, records):
        for row in records:
            n = groupCount[row[0][0]] - len(row[1])
            updated_list = list(row[1]) + [0 for i in range(n)]
            median = np.median(updated_list)
            std = np.std(updated_list)
            date = datetime.datetime(2019,1,1)+ timedelta(days=int(row[0][1]))
            if date.year == 2020:
                yield row[0][0], ','.join([str(date.year),date.strftime('%Y-%m-%d'),str(median),str(int(max(0,median-std))),str(int(median+std))])
            else: 
                yield row[0][0], ','.join([str(date.year),date.strftime('%Y-%m-%d').replace(year=2020),str(median),str(int(max(0,median-std))),str(int(median+std))])
    '''
    Transfer our code from the notebook here, however, remember to replace
    the file paths with the ones provided in the problem description.
    '''
    rddPlaces = sc.textFile('/data/share/bdm/core-places-nyc.csv')
    rddPattern = sc.textFile('/data/share/bdm/weekly-patterns-nyc-2019-2020/*')
    OUTPUT_PREFIX = sys.argv[1]
    #TO_BE_COMPLETED
    CAT_CODES ={'445210', '445110', '722410', '452311', '722513', '445120', '446110', '445299', '722515', '311811', '722511', '445230', '446191', '445291', '445220', '452210', '445292'}
    CAT_GROUP = {'452210': 0, '452311': 0, '445120': 1, '722410': 2, '722511': 3, '722513': 4, '446110': 5, '446191': 5, '722515': 6, '311811': 6, '445210': 7, '445299': 7, '445230': 7, '445291': 7, '445220': 7, '445292': 7, '445110': 8}
    rddD = rddPlaces.mapPartitionsWithIndex(functools.partial(filterPOIs,CAT_CODES,CAT_GROUP)).cache()
    storeGroup = dict(rddD.collect())
    groupCount = rddD.map(lambda x: (storeGroup[x[0]], 1)).reduceByKey(lambda x,y: x+y).sortByKey(True).map(lambda x:x[1]).collect()
    rddG = rddPattern.mapPartitionsWithIndex(functools.partial(extractVisits, storeGroup))
    rddI = rddG.groupByKey().mapPartitionsWithIndex(functools.partial(computeStats, groupCount))
    rddJ = rddI.sortBy(lambda x: x[1][:15])
    header = sc.parallelize([(-1, 'year,date,median,low,high')]).coalesce(1)
    rddJ = (header + rddJ).coalesce(10).cache()
    filenames = ['big_box_grocers','convenience_stores','drinking_places','full_service_restaurants','limited_service_restaurants','pharmacies_and_drug_stores','snack_and_bakeries','specialty_food_stores','supermarkets_except_convenience_stores']
    for filename in filenames:
        rddJ.filter(lambda x: x[0]==0 or x[0]==-1).values().saveAsTextFile(f'{OUTPUT_PREFIX}/{filename}')

if __name__=='__main__':
    sc = SparkContext()
    main(sc)