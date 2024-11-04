from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Initialize Spark Session and Context
conf = SparkConf().setAppName("FriendsTest").setMaster("local[*]")
sc = SparkContext(conf=conf)
rdd = sc.textFile("friends_test.csv")

# Process data for average number of friends by age
rdd_split = rdd.map(lambda line: line.split(","))
age_friends_rdd = rdd_split.map(lambda row: (int(row[2]), (int(row[3]), 1)))
sum_count_rdd = age_friends_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
avg_friends_by_age = sum_count_rdd.mapValues(lambda x: x[0] / x[1])

# Display Results
sorted_avg_friends_by_age = avg_friends_by_age.sortByKey()
for age, avg_friends in sorted_avg_friends_by_age.collect():
    print(f"Age: {age}, Average Number of Friends: {avg_friends:.2f}")

sc.stop()
