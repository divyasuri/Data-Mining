from pyspark import SparkContext 
import json
import sys

sc = SparkContext.getOrCreate()

def review_count(user): #function to generate key value pairs of reviews and a count
    for k,v in user.items():
        if k == 'review_count':
            return (v,1)

def usernames(user): #function to generate key value pairs of usernames and a count 
    for k,v in user.items():
        if k == 'name':
            return (v, 1)

def year_on_yelp(user): #function to generate key value pairs of year on yelp and a count
    for k,v in user.items():
        if k == 'yelping_since':
            split_year = v.split('-')
            year = int(split_year[0])
            return (year, 1)

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
	usersRDD = sc.textFile(input_file) #build initial RDD from input json 
	idRDD = usersRDD.map(lambda user: (user,1)).values().sum() #to answer part A, find sum of the values of the key value pairs generated
	json_parsed = usersRDD.map(lambda user:(json.loads(user))) #using json.loads() function to create RDD that can be parsed
	review_counts = json_parsed.map(lambda user: review_count(user))
	total_users = review_counts.values().sum() #total number of users
	total_reviews = review_counts.keys().sum() #total number of reviews
	average = total_reviews/total_users
	user_names = json_parsed.map(lambda user: usernames(user)) #extract all usernames
	distinct_names = user_names.keys().distinct() #find distinct usernames
	distinct_names_count = distinct_names.count()
	year = json_parsed.map(lambda user: year_on_yelp(user)) 
	users_in_2011_int = year.reduceByKey(lambda x,y : x+y).filter(lambda x: 2011 in x).collect() #reduce int key values and filter out pair with key 2011
	users_in_2011 = users_in_2011_int[0][1] #extract value from tuple which is in a list
	username_reduced = user_names.reduceByKey(lambda x,y: x+y) #to generate pairs of usernames and their count
	username_sort_by_value = username_reduced.map(lambda x: (x[1],x[0])).sortByKey(False) #rearrange the pair to sort by value in descending order
	top_usernames = username_sort_by_value.map(lambda x: (x[1],x[0])).take(10) #again, rearrange the pair and take top 10
	user_ID_reviews = json_parsed.map(lambda user: (userID_qF(user),review_count_qF(user))) #create pairs of user ID and their review counts
	userID_sort_by_value = user_ID_reviews.map(lambda x: (x[1],x[0])).sortByKey(False) #rearrange pairs to sort by review counts
	top_userID = userID_sort_by_value.map(lambda x: (x[1],x[0])).take(10)
	answers = dict() #to write into json 
	answers["total_users"] = idRDD
	answers["avg_reviews"] = average
	answers["distinct_usernames"] = distinct_names_count
	answers["num_users"] = users_in_2011
	answers["top10_popular_names"] = list()
	for user in top_usernames:
		data = [user[0],user[1]]
		answers["top10_popular_names"].append(data)
	answers["top10_most_reviews"] = list()
	for user_ID in top_userID:
		data1 = [user_ID[0],user_ID[1]]
		answers["top10_most_reviews"].append(data1)
	with open(output_file,'w') as outfile:
		json.dump(answers,outfile,indent=4)

main()




