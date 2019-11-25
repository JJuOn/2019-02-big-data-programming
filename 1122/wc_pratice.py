import sys
import re
from operator import add

from pyspark import SparkContext

def map_phase(x):
	x = re.sub('--', ' ', x)
	x = re.sub("'", '', x)
	return re.sub('[?!@#$\'",.;:()]', '', x).lower()

if __name__ == "__main__":
	if len(sys.argv) < 4:
		print >> sys.stderr, "Usage: wordcount <master> <inputfile> <outputfile>"
		exit(-1)
	sc = SparkContext(sys.argv[1], "python_wordcount_sorted in bigdataprogramming")
	lines = sc.textFile('hdfs://localhost:9000/user/hadoop/'+sys.argv[2],2)
	print(lines.getNumPartitions()) # print the number of partitions
	outRDD=lines.map(lambda x:(map_phase(x),1)).reduceByKey(add).filter(lambda x:x[0].find('neighborhood')==-1).filter(lambda x:x[0].find('tokyo')==-1).sortBy(lambda x:x[1]).collect()
	outRDD=sc.parallelize(outRDD)
	outRDD.saveAsTextFile('hdfs://localhost:9000/user/hadoop/'+sys.argv[3])
	
