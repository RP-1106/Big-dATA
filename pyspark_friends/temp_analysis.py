from pyspark import SparkConf, SparkContext

# Initialize Spark Context
conf = SparkConf().setAppName("TempDataset").setMaster("local[*]")
sc = SparkContext(conf=conf)
rdd = sc.textFile("temp.csv")

# Split data and remove header
rdd_header = rdd.first()
rdd_data = rdd.filter(lambda row: row != rdd_header).map(lambda row: row.split(","))

# Filter for TMIN and compute minimum temperatures
rdd_TMIN_filter = rdd_data.filter(lambda row: row[2] == "TMIN")
rdd_min_overall = rdd_TMIN_filter.map(lambda x: int(x[3])).reduce(lambda a, b: a if a < b else b)
print("Minimum temperature overall:", rdd_min_overall)

rdd_min_itemID = rdd_TMIN_filter.map(lambda x: (x[0], int(x[3]))).reduceByKey(lambda a, b: a if a < b else b)
print("Minimum temperature by ItemID:", rdd_min_itemID.collect())

rdd_min_stationID = rdd_TMIN_filter.map(lambda x: (x[1], int(x[3]))).reduceByKey(lambda a, b: a if a < b else b)
print("Minimum temperature by StationID:", rdd_min_stationID.collect())

# Filter for TMAX and compute maximum temperatures
rdd_TMAX_filter = rdd_data.filter(lambda row: row[2] == "TMAX")
rdd_max_overall = rdd_TMAX_filter.map(lambda x: int(x[3])).reduce(lambda a, b: a if a > b else b)
print("Maximum temperature overall:", rdd_max_overall)

rdd_max_itemID = rdd_TMAX_filter.map(lambda x: (x[0], int(x[3]))).reduceByKey(lambda a, b: a if a > b else b)
print("Maximum temperature by ItemID:", rdd_max_itemID.collect())

rdd_max_stationID = rdd_TMAX_filter.map(lambda x: (x[1], int(x[3]))).reduceByKey(lambda a, b: a if a > b else b)
print("Maximum temperature by StationID:", rdd_max_stationID.collect())

sc.stop()
