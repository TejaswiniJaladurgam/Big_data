{\rtf1\ansi\ansicpg1252\cocoartf2821
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 #!/usr/bin/env python\
"""\
Usage: file_dstream_homework.py <input_dir> <output_prefix>\
Reads JSON files from the input directory, extracts 'ORIGIN_COUNTRY_NAME' and 'count',\
and writes out the aggregated sum for each country.\
Example:\
    spark-submit file_dstream_homework.py /umbc/rs/is789sp25/users/your_username/hw5/input /umbc/rs/is789sp25/users/your_username/hw5/output\
"""\
\
import sys\
import json\
from pyspark import SparkContext, SparkConf\
from pyspark.streaming import StreamingContext\
\
if __name__ == "__main__":\
    if len(sys.argv) != 3:\
        print("Usage: file_dstream_homework.py <input_dir> <output_dir>", file=sys.stderr)\
        sys.exit(-1)\
\
    # 1. Initialize SparkContext and StreamingContext with a 10-second batch interval.\
    conf = SparkConf().setAppName("FileDStreamHomework")\
    sc = SparkContext(conf=conf)\
    ssc = StreamingContext(sc, 10)\
    \
    # 2. Set a checkpoint directory (this must already exist).\
    ssc.checkpoint("/umbc/rs/is789sp25/users/your_username/hw5/checkpoint")\
    \
    # 3. Monitor the input directory for new text files.\
    lines = ssc.textFileStream(sys.argv[1])\
    \
    # 4. Parse each line as JSON and extract (country, count).\
    data = lines.map(lambda line: json.loads(line)) \\\
                .map(lambda rec: (rec.get("ORIGIN_COUNTRY_NAME", "Unknown"), rec.get("count", 0)))\
    \
    # 5. Aggregate the counts for each country.\
    counts = data.reduceByKey(lambda a, b: a + b)\
    \
    # 6. Print the results to the console and save output files.\
    counts.pprint()\
    counts.saveAsTextFiles(sys.argv[2])\
    \
    # 7. Start the streaming computation.\
    ssc.start()\
    ssc.awaitTermination()\
}