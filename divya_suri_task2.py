from pyspark import SparkContext 
import json
import sys
import datetime

sc = SparkContext.getOrCreate()

def userID_qF(user): #function to extract user_ids to answer question F
    for k,v in user.items():
        if k == 'user_id':
            return v

def review_count_qF(user): #function to extract review count or each user_id to answer question F
    for k,v in user.items():
        if k == 'review_count':
            return v 

def main():
	input_file = sys.argv[1]
	output_file = sys.argv[2]
	inp_partition = sys.argv[3]
	usersRDD = sc.textFile(input_file) #build initial RDD from input json 
	json_parsed = usersRDD.map(lambda user:(json.loads(user))) #using json.loads() function to create RDD that can be parsed
	num_of_partitions1 = json_parsed.getNumPartitions() #number of partitions in initial RDD
	num_of_items1 = json_parsed.glom().map(lambda partition: len(partition)).collect() #find the length of each parition (number of items)
	start_time1 = datetime.datetime.now() #timestamp when operation starts
	user_ID_reviews1 = json_parsed.map(lambda user: (userID_qF(user),review_count_qF(user)))
	userID_sort_by_value1 = user_ID_reviews1.map(lambda x: (x[1],x[0])).sortByKey(False)
	top_userID1 = userID_sort_by_value1.map(lambda x: (x[1],x[0])).take(10)
	end_time1 = datetime.datetime.now() #timestamp when operation ends
	difference1 = end_time1 - start_time1
	difference_duration1 = round(difference1.total_seconds(),2) #execution time
	repartitioned = json_parsed.repartition(int(inp_partition)) #repartition initial RDD with customized partitions
	num_of_partitions2 = repartitioned.getNumPartitions()
	num_of_items2 = repartitioned.glom().map(lambda partition: len(partition)).collect()
	start_time2 = datetime.datetime.now() #timestamp when operation starts
	user_ID_reviews2 = repartitioned.map(lambda user: (userID_qF(user),review_count_qF(user)))
	userID_sort_by_value2 = user_ID_reviews2.map(lambda x: (x[1],x[0])).sortByKey(False)
	top_userID2 = userID_sort_by_value2.map(lambda x: (x[1],x[0])).take(10)
	end_time2 = datetime.datetime.now() #timestamp when operation ends
	difference2 = end_time2 - start_time2 
	difference_duration2 = round(difference2.total_seconds(),2) #execution time 
	answers = dict() #to write into json
	answers["default"] = dict()
	answers["default"]["n_partition"] = num_of_partitions1
	answers["default"]["n_items"] = num_of_items1
	answers["default"]["exe_time"] = str(difference_duration1) + ' seconds'
	answers["customized"] = dict()
	answers["customized"]["n_partition"] = num_of_partitions2
	answers["customized"]["n_items"] = num_of_items2
	answers["customized"]["exe_time"] = str(difference_duration2) + ' seconds'
	answers["explanation"] = "Changing the number of partitions can decrease or increase network traffic and communication across a distributed system. If the RDD has too many partitions, scheduling tasks might take long (increased execution time) while having too few partitions can result in some worker nodes remaining idle."
	with open(output_file,'w') as outfile:
		json.dump(answers,outfile,indent=4)

main()
