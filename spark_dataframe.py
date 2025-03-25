from pyspark.sql import SparkSession

# Initialize spark session
spark = SparkSession.builder.appName("SparkDataframeTasks").getOrCreate()

# Load 2010-summary and 2011-summary data into DataFrames and create temporary views
df_2010_summary = spark.read.json("/umbc/rs/is789sp25/common/data/2010-summary.json")
df_2010_summary.createOrReplaceTempView("teamGryffindor_2010_table")

df_2011_summary = spark.read.json("/umbc/rs/is789sp25/common/data/2011-summary.json")
df_2011_summary.createOrReplaceTempView("teamGryffindor_2011_table")

# Query of ORIGIN_COUNTRY_NAME for United States
df_query_US = df_2010_summary.fliter(df_2010_summary["ORIGIN_COUNTRY_NAME"] == "United States")

# Save the results to a text fiile
df_query_US.write.text("/home/xb56574/is789sp25_user/hw4/df_query_US_result.txt")

# Check if the output directory already exists and delete its contents
output_directory = "/home/xb56574/is789sp25_user/hw4/df_join_result"
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
if fs.exists(spark._jvm.org.apache.hadoop.fs.Path(output_directory)):
    fs.delete(spark._jvm.org.apache.hadoop.fs.Path(output_directory), True)

# Perform join on two dataframes between ORIGIN_COUNTRY_NAME and DEST_COUNTRY_NAME
df_join = df_2010_summary.join(df_2011_summary, df_2010_summary["ORIGIN_COUNTRY_NAME"] == df_2011_summary["DEST_COUNTRY_NAME"])

# Save the joined DataFrame to a text file with a unique name
df_join.coalesce(1).write.text(output_directory)

# Stop the Spark session
spark.stop()