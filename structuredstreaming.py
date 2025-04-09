{\rtf1\ansi\ansicpg1252\cocoartf2821
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 #!/usr/bin/env python\
"""\
Usage: structured_streaming_homework.py <input_path> <output_path>\
Reads JSON files from the input directory, extracts 'ORIGIN_COUNTRY_NAME' and 'count',\
aggregates the counts per country, and writes the complete result as JSON.\
Example:\
    spark-submit structured_streaming_homework.py /umbc/rs/is789sp25/users/your_username/hw5/input /umbc/rs/is789sp25/users/your_username/hw5/output\
"""\
\
import sys\
from pyspark.sql import SparkSession\
from pyspark.sql.types import StructType, StructField, StringType, IntegerType\
from pyspark.sql.functions import sum as spark_sum\
\
if __name__ == "__main__":\
    if len(sys.argv) != 3:\
        print("Usage: structured_streaming_homework.py <input_path> <output_path>", file=sys.stderr)\
        sys.exit(-1)\
    \
    input_path = sys.argv[1]\
    output_path = sys.argv[2]\
\
    # 1. Create a SparkSession.\
    spark = SparkSession.builder.appName("StructuredStreamingHomework").getOrCreate()\
\
    # 2. Define the schema for the JSON input.\
    schema = StructType([\
        StructField("ORIGIN_COUNTRY_NAME", StringType(), True),\
        StructField("count", IntegerType(), True)\
    ])\
\
    # 3. Read the JSON files as a streaming DataFrame.\
    df = spark.readStream.format("json").schema(schema).load(input_path)\
\
    # 4. Group by 'ORIGIN_COUNTRY_NAME' and sum the 'count' values.\
    result = df.groupBy("ORIGIN_COUNTRY_NAME").agg(spark_sum("count").alias("total_count"))\
\
    # 5. Write the complete result to the output directory using JSON format.\
    query = result.writeStream \\\
        .outputMode("complete") \\\
        .format("json") \\\
        .option("checkpointLocation", "/umbc/rs/is789sp25/users/your_username/hw5/chkpt_structured") \\\
        .option("path", output_path) \\\
        .start()\
\
    query.awaitTermination()\
}