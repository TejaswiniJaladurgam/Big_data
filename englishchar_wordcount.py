import re
import sys
from pyspark.sql import SparkSession

if __name__=="__main__":
	spark=SparkSession\
	.builder\
	.appName("EnglishCharWordCount")\
	.getOrCreate()
	sc=spark.sparkContext

	data=sys.argv[1]
	output="/umbc/rs/is789sp25/users/xb56574/home-work2/englishchar_wordcount-output"
	lines=spark.read.text(data).rdd.map(lambda row:row[0])
	extracted_words=lines.flatMap(lambda l:re.findall(r"[A-Za-z]+",l))
	words=extracted_words.map(lambda w: (w.lower(),1))
	word_counts=words.reduceByKey(lambda x, y:x+y)
	res=word_counts.collect()
	for(word, count) in res:
		print(word, ":", count)
	word_counts.saveAsTextFile(output)
	spark.stop()
