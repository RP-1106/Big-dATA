from pyspark import SparkContext

# Initialize Spark context
sc = SparkContext("local", "Temp-Min-Max")

# Load the temp.csv dataset
temp_rdd = sc.textFile("/home/snucse/Desktop/temp.csv")

# Skip the header and parse the data
header = temp_rdd.first()
data_rdd = temp_rdd.filter(lambda line: line != header)

# Filter for TMIN values and map to (ItemID, temperature)
tmin_rdd = data_rdd.filter(lambda line: "TMIN" in line) \
                   .map(lambda line: line.split(",")) \
                   .map(lambda cols: (cols[0], float(cols[1])))  # (ItemID, temperature)

# a. Overall minimum temperature
overall_min_temp = tmin_rdd.map(lambda x: x[1]).min()

# b. Minimum temperature for every ItemID
min_temp_by_item = tmin_rdd.reduceByKey(lambda x, y: min(x, y))

# c. Minimum temperature for every StationID
min_temp_by_station = data_rdd.filter(lambda line: "TMIN" in line) \
                               .map(lambda line: line.split(",")) \
                               .map(lambda cols: (cols[2], float(cols[1])))  # (StationID, temperature)
min_temp_by_station = min_temp_by_station.reduceByKey(lambda x, y: min(x, y))

# Collect and print results
print(f"Overall Minimum Temperature: {overall_min_temp}")
print("Minimum Temperature by ItemID:")
for item_id, min_temp in min_temp_by_item.collect():
    print(f"ItemID: {item_id}, Min Temp: {min_temp}")

print("Minimum Temperature by StationID:")
for station_id, min_temp in min_temp_by_station.collect():
    print(f"StationID: {station_id}, Min Temp: {min_temp}")

# Filter for TMAX values and map to (ItemID, temperature)
tmax_rdd = data_rdd.filter(lambda line: "TMAX" in line) \
                   .map(lambda line: line.split(",")) \
                   .map(lambda cols: (cols[0], float(cols[1])))  # (ItemID, temperature)

# a. Overall maximum temperature
overall_max_temp = tmax_rdd.map(lambda x: x[1]).max()

# b. Maximum temperature for every ItemID
max_temp_by_item = tmax_rdd.reduceByKey(lambda x, y: max(x, y))

# c. Maximum temperature for every StationID
max_temp_by_station = data_rdd.filter(lambda line: "TMAX" in line) \
                               .map(lambda line: line.split(",")) \
                               .map(lambda cols: (cols[2], float(cols[1])))  # (StationID, temperature)
max_temp_by_station = max_temp_by_station.reduceByKey(lambda x, y: max(x, y))

# Collect and print results
print(f"Overall Maximum Temperature: {overall_max_temp}")
print("Maximum Temperature by ItemID:")
for item_id, max_temp in max_temp_by_item.collect():
    print(f"ItemID: {item_id}, Max Temp: {max_temp}")

print("Maximum Temperature by StationID:")
for station_id, max_temp in max_temp_by_station.collect():
    print(f"StationID: {station_id}, Max Temp: {max_temp}")
