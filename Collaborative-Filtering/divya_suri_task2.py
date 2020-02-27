from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, Rating
import math 
import datetime
import sys

def AddRatingsDict(user): #create a dictionary for each user with the business as key and their rating as the value
	    ratings_dict = {}
	    ratings = user[1]
	    for rating in ratings:
	        if rating[0] not in ratings_dict:
	            ratings_dict[rating[0]] = rating[1]
	        else:
	            ratings_dict[rating[0]] = ratings_dict[rating[0]].append(rating[1])
	    return (user[0],ratings_dict) #return each user and their ratings dictionary

def GetPearsonCorrelation(user,other,train_data2_amend): #gives the pearson correlation for two users
	other_user_ratings = train_data2_amend[other] #ratings of the other user
	cur_user_ratings = train_data2_amend[user] #ratings of the current user
	corrated_businesses = [value for value in cur_user_ratings.keys() if value in other_user_ratings.keys()] #finds the corrated businesses between the users
	if len(corrated_businesses) == 0: # if no corrated businesses, correlation is 0 
		return 0 
	else: 
		num_corrated = len(corrated_businesses)
		cur_ratings = []
		other_ratings = []
		for co in corrated_businesses:
			cur_data = train_data2_amend[user]
			other_data = train_data2_amend[other]
			cur_rating = cur_data[co]
			cur_ratings.append(cur_rating)
			other_rating = other_data[co]
			other_ratings.append(other_rating)
		cur_mean = sum(cur_ratings)/num_corrated
		other_mean = sum(other_ratings)/num_corrated 
		normalized_cur_values = [v- cur_mean for v in cur_ratings] #normalize all ratings with mean 
		normalized_other_values = [x - other_mean for x in other_ratings ]
		numerator = 0 
		denominator1 = 0 
		denominator2 = 0 
		for i in range(len(normalized_cur_values)): #generate numerator and denominator for correlation 
			numerator = numerator + (normalized_cur_values[i]*normalized_other_values[i])
			denominator1 = denominator1 + (normalized_cur_values[i] * normalized_cur_values[i])
			denominator2 = denominator2 + (normalized_other_values[i] * normalized_other_values[i])
		denominator1 = math.sqrt(denominator1)
		denominator2 = math.sqrt(denominator2)
		denominator = denominator1 * denominator2
		if denominator == 0:
			return 0 
		else:
			val = numerator/denominator
			return val

def FindSimilarUsers(user, business,businesses_data,train_data2_amend): #findings all the pearson correlations to determine similar users
	similar = []
	if business not in businesses_data: #business has not been rated yet 
	    similar.append(0) #similarity with particular user is 0 
	    return similar
	other_users = businesses_data[business]
	for other in other_users:
	    if other != user: 
	        sim = GetPearsonCorrelation(user,other,train_data2_amend) #similarity of user with other users 
	    if sim > 0:
	    	similar.append((user,other,sim))
	if len(similar) == 0: #no correlated businesses 
	    similar.append(0)
	    return similar
	return similar

def PredictRatings(user,business,similar_users,train_data2_amend): #predict ratings in the test data 
	user_data = train_data2_amend[user] #current user ratings for each business
	user_ratings = [] #gather all ratings
	for k,v in user_data.items():
		if k == business:
			continue
		else:
			user_ratings.append(v)
	average_user_ratings = sum(user_ratings)/len(user_ratings)
	if similar_users[0] == 0: #pearson correlation was 0 (perhaps no correlated businesses)
		return average_user_ratings
	else:
		numerator = 0 
		denominator = 0 
		for sim_use in similar_users: 
			if sim_use == 0:
				return average_user_ratings
			other_sim_use = sim_use[1]
			similarity = sim_use[2]
			sim_use_ratings = []
			sim_use_data = train_data2_amend[other_sim_use]
			for k,v in sim_use_data.items():
				if k == business:
					bus_rating = v 
				else:
					sim_use_ratings.append(v )
			average_sim_use_ratings = sum(sim_use_ratings)/len(sim_use_ratings) #average similarity 
			step = (bus_rating - average_sim_use_ratings)*similarity
			numerator = numerator + step 
			denominator = denominator + similarity 
		if denominator == 0:
			return average_user_ratings
		else:
			predict = average_user_ratings + (numerator/denominator)
			return predict

def TransformTrain(train,users_dict,business_dict): #for Model Based, transform the users and business IDs to integers based on dictionaries
    user_id = train[0]
    business_id = train[1]
    rating = train[2]
    return (users_dict[user_id],business_dict[business_id],float(rating))

def TransformTest(test,users_dict,business_dict): #for Model Based, transform the users and business IDs to integers based on dictionaries 
    user_id = test[0]
    business_id = test[1]
    rating = test[2]
    if user_id not in users_dict: #user has not given any rating 
        trans_user = -2 
    else:
        trans_user = users_dict[user_id]
    if business_id not in business_dict: #business has not been rated
        trans_business  = -2
    else:
        trans_business  = business_dict[business_id]
    return (trans_user,trans_business,float(rating)) 

def ReTransform(pred, flipped_users_dict,flipped_businesses_dict): #for output file, convert all integers back to string IDs
    user_id = pred[0][0]
    business_id = pred[0][1]
    return (flipped_users_dict[user_id],flipped_businesses_dict[business_id],pred[1])

def UserBased(): #implements user based cf
	train_RDD = cf.textFile(train_file)
	test_RDD = cf.textFile(test_file)
	train_header = train_RDD.first()
	test_header = test_RDD.first()
	train_RDD = train_RDD.filter(lambda x:x!= train_header) #remove header of file
	test_RDD = test_RDD.filter(lambda x:x!= test_header)
	train_RDD = train_RDD.map(lambda x:x.split(','))
	test_RDD = test_RDD.map(lambda x:x.split(','))
	train_data = train_RDD.map(lambda x:((x[0], (x[1])), float(x[2])))
	test_data = test_RDD.map(lambda x:((x[0], (x[1])), float(x[2])))
	businesses_data = train_data.map(lambda x:(x[0][1],x[0][0])).groupByKey().map(lambda x: (x[0],list(x[1]))).collectAsMap() #gather all the businesses and the users that have rated them 
	train_data2 = train_data.map(lambda x: (x[0][0],(x[0][1],x[1]))).groupByKey().map(lambda x : (x[0], list(x[1]))) #create main RDD with user and list of businesses they rated
	train_data2_amend = train_data2.map(lambda x:AddRatingsDict(x)).collectAsMap() #convert value to dictionary
	similar_users = test_data.map(lambda x: (x[0][0],x[0][1],FindSimilarUsers(x[0][0],x[0][1],businesses_data,train_data2_amend))) #find similarities
	pred = similar_users.map(lambda x: ((x[0], x[1]), PredictRatings(x[0], x[1], x[2],train_data2_amend))) #generate predictions
	pred2 = pred.collect() #collect all predictions
	results = test_data.join(pred) #join test data with predicted results
	end_time = datetime.datetime.now()
	difference1 = end_time - start_time
	difference_duration1 = round(difference1.total_seconds(),2) 
	rmse_differences = results.map(lambda x:(abs(x[1][0] - x[1][1]))**2).mean() #calculate rmse 
	rmse = math.sqrt(rmse_differences)
	with open(output_file,'w') as outfile:
			outfile.write('user_id')
			outfile.write(",")
			outfile.write('business_id')
			outfile.write(",")
			outfile.write("prediction")
			outfile.write('\n')
			for p in pred2:
				outfile.write(str(p[0][0]))
				outfile.write(",")
				outfile.write(str(p[0][1]))
				outfile.write(",")
				outfile.write(str(p[1]))
				outfile.write("\n")
	outfile.close()
	print('RMSE: ' + str(rmse))
	print('Duration: ' + str(difference_duration1) +' seconds')

def ModelBased(): #implement model based CF
	train_RDD = cf.textFile('yelp_train.csv')
	test_RDD = cf.textFile('yelp_val.csv')
	train_header = train_RDD.first()
	test_header = test_RDD.first()
	train_RDD = train_RDD.filter(lambda x:x!= train_header) #remove header
	test_RDD = test_RDD.filter(lambda x:x!= test_header)
	train_RDD = train_RDD.map(lambda x:x.split(','))
	test_RDD = test_RDD.map(lambda x:x.split(','))
	users = train_RDD.map(lambda x: x[0]).zipWithIndex().collect() #to convert users to integers
	businesses = train_RDD.map(lambda x: x[1]).zipWithIndex().collect() #to convert businesses to integers
	users_dict = {}
	for user in users: #build user to integer mapping
		if user[0] not in users_dict:
			users_dict[user[0]] = user[1]
		else:
			continue
	businesses_dict = {}
	for business in businesses: #build business to integer mapping
		if business[0] not in businesses_dict:
			businesses_dict[business[0]] = business[1]
		else:
			continue
	transformed_train_RDD = train_RDD.map(lambda x: (TransformTrain(x,users_dict, businesses_dict))) #transform strings to integers
	transformed_test_RDD = test_RDD.map(lambda x: (TransformTest(x,users_dict,businesses_dict)))
	train_data = transformed_train_RDD.map(lambda x: Rating(x[0],x[1],float(x[2])))
	test_data = transformed_test_RDD.map(lambda x: (x[0],x[1]))
	rank = 10 
	iterations = 10 
	lambda_score = 0.1
	model = ALS.train(train_data, rank, iterations, lambda_score) #create model
	predictions = model.predictAll(test_data).map(lambda x: ((x[0],x[1]),x[2])) #use model to predict on test data
	transformed_test_RDD2 = transformed_test_RDD.map(lambda x: ((x[0],x[1]),x[2])) #transform test data 
	true_and_pred = transformed_test_RDD2.join(predictions)
	RMSE_int = true_and_pred.map(lambda x: (abs(x[1][0] - x[1][1]))**2).mean() #calculate RMSE
	RMSE = math.sqrt(RMSE_int)
	flipped_users_dict = {y:x for x,y in users_dict.items()} #to convert integers to string for output file
	flipped_businesses_dict = {y:x for x,y in businesses_dict.items()}
	transformed_predictions = predictions.map(lambda x: (ReTransform(x,flipped_users_dict,flipped_businesses_dict)))
	pred2 = transformed_predictions.collect()
	with open(output_file,'w') as outfile:
		outfile.write('user_id')
		outfile.write(",")
		outfile.write('business_id')
		outfile.write(",")
		outfile.write("prediction")
		outfile.write('\n')
		for p in pred2:
			outfile.write(str(p[0]))
			outfile.write(",")
			outfile.write(str(p[1]))
			outfile.write(",")
			outfile.write(str(p[2]))
			outfile.write("\n")
	outfile.close()
	end_time = datetime.datetime.now()
	difference1 = end_time - start_time
	difference_duration1 = round(difference1.total_seconds(),2)
	print('RMSE: ' + str(RMSE))
	print('Duration: ' + str(difference_duration1) +' seconds')

if __name__ == "__main__":
	start_time = datetime.datetime.now()
	train_file = sys.argv[1]
	test_file = sys.argv[2]
	case = sys.argv[3]
	output_file = sys.argv[4]
	cf = SparkContext.getOrCreate()
	if case == str(1):
		ModelBased()
	elif case == str(2): 
		UserBased()
	else:
		print('Did not implement')


