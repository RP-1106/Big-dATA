from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "Word Count")

# Read input file
input_file = "/home/snucse/Desktop/wordcount_input.txt"  # Replace with your input file path
text_file = sc.textFile(input_file)

# Count words
counts = text_file.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)

# Collect and print results
for word, count in counts.collect():
    print(f"{word}: {count}")

# Stop the SparkContext
sc.stop()
