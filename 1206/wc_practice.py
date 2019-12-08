import sys
import re
from operator import add

from pyspark import SparkContext

def map_phase(x):
	x = re.sub('--', ' ', x)
	x = re.sub("'", '', x)
	return re.sub('[?!@#$\'",.;:()]', '', x).lower()

def filter_tokyo(line):
	line=map_phase(line)
	global tokyo_count
	if line=='tokyo':
		tokyo_count+=1
	return (line,1)

if __name__ == "__main__":
	if len(sys.argv) < 4:
		print >> sys.stderr, "Usage: wordcount <master> <inputfile> <outputfile>"
		exit(-1)
	sc = SparkContext(sys.argv[1], "python_wordcount_sorted in bigdataprogrammiing")
	tokyo_count=sc.accumulator(0)
	lines = sc.textFile(sys.argv[2],2)
	print(lines.getNumPartitions()) # print the number of partitions
	outRDD=lines.map(filter_tokyo)
	outRDD=outRDD.reduceByKey(add)
	outRDD=outRDD.filter(lambda x:x[0].find('neighborhood')==-1)
	outRDD=outRDD.filter(lambda x:x[0].find('tokyo')==-1)
	outRDD=outRDD.sortBy(lambda x:x[1])
	outRDD.saveAsTextFile(sys.argv[3])
	print("Number of Tokyo : {}".format(tokyo_count))
	
