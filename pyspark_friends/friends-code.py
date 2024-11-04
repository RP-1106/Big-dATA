from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *

sc = SparkContext("local","friends")
data = sc.textFile("friends_test.csv")
data.first()

data = data.map(lambda line : line.split(','))
for i in data.take(5):
	print(i)

#AVERAGE NUMBER OF FRIENDS FOR EACH UNIQUE AGE PRESENT
df_age = data.map(lambda row: ( int(row[2]), (int(row[3]), 1 ) ) )
df_sum = df_age.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]) )

avg_by_age = df_sum.mapValues(lambda x: x[0] / x[1])

avg_by_age_sort = avg_by_age.sortByKey()

for age, avg_friends in avg_by_age_sort.collect():
	print(f"Age: {age}, Average Number of Friends: {avg_friends:.2f}")

df_age = data.map(lambda row: (int(row[2]), int(row[3])))
max_friends_by_age = df_age.reduceByKey(lambda a, b: a if a > b else b)
min_friends_by_age = df_age.reduceByKey(lambda a, b: a if a < b else b)

max_min_friends_by_age = max_friends_by_age.join(min_friends_by_age)

sorted_max_min_friends_by_age = max_min_friends_by_age.sortByKey()

for age, (max_friends, min_friends) in sorted_max_min_friends_by_age.collect():
	print(f"Age: {age}, Max Friends: {max_friends}, Min Friends: {min_friends}")

sc.stop()
