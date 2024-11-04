from pyspark import SparkContext

# Initialize Spark context
sc = SparkContext("local", "Friends Test")

# Load the dataset
friends_rdd = sc.textFile("/home/snucse/Desktop/friends_test.csv")

# Skip the header and parse the data
header = friends_rdd.first()
data_rdd = friends_rdd.filter(lambda line: line != header)

# Map to (age, num_friends) pairs
age_friends_rdd = data_rdd.map(lambda line: line.split(",")) \
                            .map(lambda cols: (int(cols[2]), int(cols[3])))  # (age, num_friends)

# Aggregate by age to calculate total friends and counts
age_friends_agg = age_friends_rdd.aggregateByKey((0, 0), 
    lambda acc, num_friends: (acc[0] + num_friends, acc[1] + 1),  # Sum and count
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])   # Combine results
)

# Calculate average number of friends
avg_friends_by_age = age_friends_agg.mapValues(lambda x: x[0] / x[1])

# Collect and print results
results = avg_friends_by_age.collect()
for age, avg_friends in results:
    print(f"Age: {age}, Average Friends: {avg_friends:.2f}")
