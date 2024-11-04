from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *

sc = SparkContext("local","temperature")
data = sc.textFile("temp.csv")

data_header = data.first()
print("--------DATA HEADER : ",data_header)

#----REMOVE HEADER--------
data = data.filter(lambda row: row!= data_header)
data2 = data.map(lambda row : row.split(","))
print("---------DATA-------")
print(data2.take(10))

#-----FILTER FOR TMIN AND COMPUTE MINIMUM TEMPERATURE-----
data_tmin = data2.filter(lambda row: row[2]=='TMIN')

min_overall = data_tmin.map(lambda x:int(x[3]))
min_overall = min_overall.reduce(lambda a,b: a if a<b else b)
print("--------MIN TEMP OVERALL----- : ", min_overall)

min_item_id = data_tmin.map(lambda x : (x[0], int(x[3])) )
min_item_id = min_item_id.reduceByKey(lambda a,b : a if a<b else b)
print("--------MIN TEMP ITEMID------ : ", min_item_id.collect())

min_station_id = data_tmin.map(lambda x : (x[1], int(x[3])) )
min_station_id = min_station_id.reduceByKey(lambda a,b : a if a<b else b)
print("--------MIN TEMP STATIONID -- : ", min_station_id.collect())

sc.stop()
