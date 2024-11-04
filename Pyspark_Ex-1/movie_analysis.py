from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MovieLens Ratings Distribution") \
    .getOrCreate()

# Load the MovieLens dataset (change the path accordingly)
ratings = spark.read.csv("/home/snucse/Desktop/movielens.csv", header=True, inferSchema=True)

# Show the original DataFrame and column names
ratings.show()
print(ratings.columns)

# Clean up column names (if needed)
ratings = ratings.toDF(*[c.strip() for c in ratings.columns])

# Calculate the count of ratings and average rating for each movieId
rating_distribution = ratings.groupBy("movieId").agg(
    F.count("rating").alias("rating_count"),
    F.avg("rating").alias("average_rating")
)

# Show the results
rating_distribution.show()

# Stop the Spark session
spark.stop()
