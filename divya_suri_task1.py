import os #import necessary functions
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
from graphframes import * 
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from functools import reduce
from pyspark.sql.functions import col, lit, when
import datetime
import sys


def Graph(): #create graph using GraphFrames
	lines = gf.textFile(input_file)
	parts = lines.map(lambda l: l.split(" "))
	edges = parts.map(lambda p:(p[0],p[1])).map(lambda p: Row(src=p[0],dst=p[1])) 
	edges_SQL = sqlContext.createDataFrame(edges, ["dst","src"]) #create edges SQL dataframe
	all_nodes = parts.collect() 
	distinct_nodes = [] #find all dictinct nodes
	for n in all_nodes:
	    for x in n:
	        if x not in distinct_nodes:
	            distinct_nodes.append(x)
	vertices_RDD = gf.parallelize(distinct_nodes) #build vertices RDD
	nodes = vertices_RDD.map(lambda p: Row(id=p))
	nodes_SQL = sqlContext.createDataFrame(nodes, ["id"])
	g = GraphFrame(nodes_SQL,edges_SQL) #create GraphFrame object
	result = g.labelPropagation(maxIter=5)
	results = result.collect()
	communities = {} #steps to extract communities
	for row in results:
		node = row.id 
		label = row.label
		if label not in communities:
			communities[label] = []
			communities[label].append(node)
		else:
			communities[label].append(node)
	com_list = []
	for k,v in communities.items():
		com_list.append(sorted(v))
	maxLength = 0 
	for com in com_list:
		if len(com) >= maxLength:
			maxLength = len(com)
	community_length = {} #partition data by len of community
	for i in range(1,maxLength + 1):
		community_length[i] = []
	for com in com_list:
		for k in community_length.keys():
			if len(com) == k:
				community_length[k].append(com)
	final = []
	for k2,v2 in community_length.items():
		final.append(sorted(v2))
	with open(output,'w') as outfile: #write out into output file
		for lst in final:
			for x in lst:
				if len(x) == 1:
					outfile.write("'")
					outfile.write(x[0])
					outfile.write("'")
					outfile.write('\n')
				else:
					for i in x:
						outfile.write("'")
						outfile.write(i)
						outfile.write("'")
						if i != x[-1]:
							outfile.write(', ')
					outfile.write('\n')
	outfile.close()
	end_time = datetime.datetime.now()
	difference1 = end_time - start_time
	difference_duration1 = round(difference1.total_seconds(),2)
	print('Duration: ' + str(difference_duration1) +' seconds')

if __name__ == "__main__":
	start_time = datetime.datetime.now()
	input_file = sys.argv[1]
	output = sys.argv[2]
	gf = SparkContext.getOrCreate()
	sqlContext = SQLContext(gf)	
	Graph()