from pyspark import SparkContext 
import json
import sys
import datetime

sc = SparkContext.getOrCreate()

def businesses(business): #function to generate pairs of business ids and their state
    for k,v in business.items():
        if k == 'business_id':
            bus_id = v
        if k == 'state':
            state = v 
            return (bus_id, state)

def reviews(review): #function to generate pairs of business ids and rating for each review
    for k,v in review.items():
        if k == 'business_id':
            bus_id = v
        if k == 'stars':
            rating = v
            return (bus_id,(rating,1))

def state_avg_reviews(business): #function to generates pairs of state, the sum of reviews for the business and the number of businesses
    state = business[1][0]
    bus_sum = business[1][1][0]
    bus_num = business[1][1][1]
    return (state, (bus_sum,bus_num))

def average_review(state): #function to generate pairs of state and their average stars
    state_name = state[0]
    state_sum = state[1][0]
    state_num = state[1][1]
    average = state_sum/state_num
    return (state_name,average)

def main():
	review_file = sys.argv[1]
	business_file = sys.argv[2]
	output_fileA = sys.argv[3]
	output_fileB = sys.argv[4]
	reviewsRDD = sc.textFile(review_file)
	businessRDD = sc.textFile(business_file)
	reviews_json_parsed = reviewsRDD.map(lambda review:(json.loads(review)))
	business_json_parsed = businessRDD.map(lambda business:(json.loads(business)))
	businesses_states = business_json_parsed.map(lambda business: businesses(business))
	businesses_reviews = reviews_json_parsed.map(lambda review: reviews(review))
	review_counts_by_business = businesses_reviews.reduceByKey(lambda x,y : ((x[0]+y[0]),(x[1]+y[1]))) #reduce to generate pairs of bus_id, the total sum of the ratings and the number of ratings
	joined = businesses_states.join(review_counts_by_business) #join the two RDDs by bus_id 
	joined_reviews_sum_count = joined.map(lambda business: state_avg_reviews(business)) #generate intermediate pairs 
	review_calc_by_state = joined_reviews_sum_count.reduceByKey(lambda x,y : ((x[0]+y[0]),(x[1]+y[1]))) #reduce to generate pairs of state, the total sum of the ratings for all businesses in that state and the number of businesses in the state
	state_averages = review_calc_by_state.map(lambda state: average_review(state)).sortByKey() #sort by state in ascending order fitst
	state_averages_sort_by_value = state_averages.map(lambda x: (x[1],x[0])).sortByKey(False) #rearrange pairs to sort by ratings in descending order
	state_averages_ordered = state_averages_sort_by_value.map(lambda x: (x[1],x[0])) #rearrange in correct order of pairs
	state_averages_ordered_collected= state_averages_sort_by_value.map(lambda x: (x[1],x[0])).collect() #collect data to write into output file
	start_time1 = datetime.datetime.now() #for part B, to compare execution times
	top5_averages_m1 = state_averages_ordered.collect()[0:5] #print first 5 items of the list
	end_time1 = datetime.datetime.now()
	difference1 = end_time1 - start_time1
	difference_duration1 = round(difference1.total_seconds(),2)
	start_time2 = datetime.datetime.now()
	top5_averages_m2 = state_averages_ordered.take(5) #take first 5 items of RDD
	end_time2 = datetime.datetime.now()
	difference2 = end_time2 - start_time2
	difference_duration2 = round(difference2.total_seconds(),2)
	with open(output_fileA,'w') as outfile:
		outfile.write('state,stars')
		outfile.write('\n')
		for state in state_averages_ordered_collected:
			state_acr = state[0]
			state_stars = str(state[1])
			outfile.write(state_acr)
			outfile.write(',')
			outfile.write(state_stars)
			outfile.write('\n')
		outfile.close()
	answers = dict() #to write in json
	answers["m1"] = str(difference_duration1) + ' seconds'
	answers["m2"] = str(difference_duration2) + ' seconds'
	answers["explanation"] = "Collecting the data first converts the output into a list and then allows you to extract the necessary items from it. By taking the first 5 items, you skip the step of converting the output into a list which reduces the execution time."
	with open(output_fileB,'w') as outfile2:
		json.dump(answers,outfile2,indent=4)

main()
