import pandas as pd
import math 
import datetime
import sys
import json 
import numpy as np 
from pyspark import SparkContext
from numpy import dot
from numpy.linalg import norm

def AddUserRatingsDict(user): 
    ratings_dict2 = {}
    ratings2 = user[1]
    for rating in ratings2:
        if rating[0] not in ratings_dict2:
            ratings_dict2[rating[0]] = rating[1]
        else:
            ratings_dict2[rating[0]] = ratings_dict2[rating[0]].append(rating[1])
    return (user[0],ratings_dict2) 

def AddBusRatingsDict(business,): 
    ratings_dict = {}
    ratings = business[1]
    for rating in ratings:
        if rating[0] not in ratings_dict:
            ratings_dict[rating[0]] = rating[1]
        else:
            ratings_dict[rating[0]] = ratings_dict[rating[0]].append(rating[1])
    return (business[0],ratings_dict) 

def UserAverages(user):
    user_id = user[0]
    user_ratings = user[1]
    user_ratings = list(user_ratings.values())
    user_average = sum(user_ratings)/len(user_ratings)
    return (user_id,user_average)

def AdjustRatings(x,user_averages):
    user_ratings = x[1]
    for k,v in user_ratings.items():
        v = v - (user_averages[k])
        user_ratings[k] = v
    return (x[0],user_ratings)

def RestaurantMarker(business):
    try: 
        categories = business['categories']
        if 'Restaurants' in categories or 'Food' in categories:
            return (business['business_id'],1) 
        else:
            return (business['business_id'],0)
    except:
        return (business['business_id'],0)

def BeautyMarker(business):
    try: 
        categories = business['categories']
        if 'Beauty & Spas' in categories:
            return (business['business_id'],1) 
        else:
            return (business['business_id'],0)
    except:
        return (business['business_id'],0)

def StarsMarker(business):
    try:
        stars = business['stars']
        return (business['business_id'],stars)
    except:
        return (business['business_id'],-1)

def OpenMarker(business):
    try:
        is_open = business['is_open']
        if business['is_open'] == None:
            return (business['business_id'],1)
        else:
            return (business['business_id'],is_open)
    except:
        return (business['business_id'],0)

def LocalMarker(business):
    try: 
        categories = business['categories']
        if 'Local Services' in categories:
            return (business['business_id'],1) 
        else:
            return (business['business_id'],0)
    except:
        return (business['business_id'],0)

def AddCatMarker(business,res_dict,stars_dict,open_dict,local_dict,beauty_dict):#res_dict,beauty_dict,stars_dict,open_dict,local_dict
    business_id = business[0]
    bus_dict = business[1]
    if business_id in res_dict:
        bus_dict['Rest'] = res_dict[business_id]
    else:
        bus_dict['Rest'] = 0
    if business_id in beauty_dict:
        bus_dict['Beauty'] = beauty_dict[business_id]
    else:
        bus_dict['Beauty'] = 0
    if business_id in stars_dict:
        bus_dict['Stars'] = stars_dict[business_id]
    else:
        bus_dict['Stars'] = -1
    if business_id in open_dict:
        bus_dict['Open'] = open_dict[business_id]
    else:
        bus_dict['Open'] = 0
    if business_id in local_dict:
        bus_dict['Local'] = local_dict[business_id]
    else:
        bus_dict['Local'] = 0
    return (business_id,bus_dict)

def CosineSimilarity(business,other,businesses_data,user_averages):
    other_users = businesses_data[other]
    cur_users = businesses_data[business]
    corrated_users = [value for value in cur_users.keys() if value in other_users.keys()] 
    if len(corrated_users) == 0:
        return 0 
    else:
        other_list = []
        cur_list = []
        cur_data = businesses_data[business]
        other_data = businesses_data[other]
        for co in corrated_users:
            cur_rating = cur_data[co]
            cur_list.append(cur_rating)
            other_rating = other_data[co]
            other_list.append(other_rating) 
        cur_list.append(cur_data['Rest']) 
        other_list.append(other_data['Rest'])
        cur_list.append(cur_data['Beauty'])
        other_list.append(other_data['Beauty'])
        cur_list.append(cur_data['Stars'])
        other_list.append(other_data['Stars'])
        cur_list.append(cur_data['Open'])
        other_list.append(other_data['Open'])
        cur_list.append(cur_data['Local'])
        other_list.append(other_data['Local'])
        cosine = dot(cur_list, other_list)/(norm(cur_list)*norm(other_list))
        return cosine.item()
        
def FindSimilarBusinesses(user, business,users_data,businesses_data,user_averages): 
    similar = []
    if business not in businesses_data: 
        similar.append(0) 
        return similar
    other_businesses = users_data[user]
    for other in other_businesses:
        if other != business: 
            sim = CosineSimilarity(business,other,businesses_data,user_averages)#similarity of user with other users 
        if sim > 0:
            similar.append((business,other,sim))
    if len(similar) == 0:
        similar.append(0)
        return similar
    return similar

def PredictRatings(user, business, similar_businesses,users_data,userBusiness):
    user_data = users_data[user]
    user_ratings = []
    for k,v in user_data.items():
        if k == business:
            continue 
        else:
            user_ratings.append(v)
    average_user_ratings = sum(user_ratings)/len(user_ratings)
    if similar_businesses[0] == 0:
        return average_user_ratings 
    if len(similar_businesses) == 1:
        return average_user_ratings
    if (len(similar_businesses) == 1) and (similar_businesses[0][1] == business):
        return average_user_ratings
    else:
        numerator = 0 
        denominator = 0 
        for sim_bus in similar_businesses:
            if sim_bus == 0:
                return average_user_ratings
            else:
                other = sim_bus[1]
                sim = sim_bus[2]
                amp_sim = sim * (abs(sim**(2.5-1)))
                key = (user,other)
                if key in userBusiness:
                    rating = userBusiness[key]
                    numerator  = numerator + (float(rating)*float(amp_sim))
                    denominator = denominator + abs(amp_sim)
        if denominator == 0 or numerator == 0:
            return average_user_ratings
        else:
            rating = numerator/denominator
            if rating < 0:
                rating = -1 * rating
    return rating 


start_time = datetime.datetime.now()
path_to_folder = sys.argv[1]
train_file = str(path_to_folder) + 'yelp_train.csv'
business_file = str(path_to_folder) + 'business.json'
test_file = sys.argv[2]
output_file = sys.argv[3]
SparkContext.setSystemProperty('spark.driver.memory', '10g')
ibcf12 = SparkContext.getOrCreate()
train_RDD = ibcf12.textFile(train_file)
test_RDD = ibcf12.textFile(test_file)
business_RDD = ibcf12.textFile(business_file)
train_header = train_RDD.first()
test_header = test_RDD.first()
train_RDD = train_RDD.filter(lambda x:x!= train_header) #remove header of file
test_RDD = test_RDD.filter(lambda x:x!= test_header)
train_RDD = train_RDD.map(lambda x:x.split(','))
test_RDD = test_RDD.map(lambda x:x.split(','))
train_data = train_RDD.map(lambda x:((x[0], (x[1])), float(x[2])))
test_data = test_RDD.map(lambda x:((x[0], (x[1])), float(x[2])))
userBusiness = train_RDD.map(lambda x : ((x[0], x[1]), x[2])).sortByKey().collectAsMap()
business_json_parsed = business_RDD.map(lambda business:(json.loads(business)))
restaurants_parsed = business_json_parsed.map(lambda business:RestaurantMarker(business))
local_parsed = business_json_parsed.map(lambda business:LocalMarker(business))  
beauty_parsed = business_json_parsed.map(lambda business:BeautyMarker(business))
stars_parsed = business_json_parsed.map(lambda business:StarsMarker(business))
open_parsed = business_json_parsed.map(lambda business:OpenMarker(business))
local_data = local_parsed.collect()
beauty_data = beauty_parsed.collect()
res_data = restaurants_parsed.collect()
stars_data = stars_parsed.collect()
open_data = open_parsed.collect()
res_dict = {}
for r in res_data:
    if r[0] not in res_dict:
        res_dict[r[0]] = r[1]
beauty_dict = {}
for b in beauty_data:
    if b[0] not in beauty_dict:
        beauty_dict[b[0]] = b[1]
stars_dict = {}
for s in stars_data:
    if s[0] not in stars_dict:
        stars_dict[s[0]] = s[1]
open_dict = {}
for o in open_data:
    if o[0] not in open_dict:
        open_dict[o[0]] = o[1]
local_dict = {}
for l in local_data:
    if l[0] not in local_dict:
        local_dict[l[0]] = l[1]
users_data_int = train_data.map(lambda x: (x[0][0],(x[0][1],x[1]))).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x:AddUserRatingsDict(x))
user_averages = users_data_int.map(lambda x:UserAverages(x)).collectAsMap()
users_data = users_data_int.collectAsMap()
businesses_data1 = train_data.map(lambda x:(x[0][1],(x[0][0],x[1]))).groupByKey().map(lambda x: (x[0],list(x[1]))).map(lambda x:AddBusRatingsDict(x)).map(lambda x:AdjustRatings(x,user_averages))
businesses_data = businesses_data1.map(lambda x: AddCatMarker(x,res_dict,stars_dict,open_dict,local_dict,beauty_dict)).collectAsMap()
similar_businesses = test_data.map(lambda x: (x[0][0],x[0][1],FindSimilarBusinesses(x[0][0],x[0][1],users_data,businesses_data,user_averages)))
pred = similar_businesses.map(lambda x: ((x[0], x[1]), PredictRatings(x[0], x[1], x[2],users_data,userBusiness)))
pred2 = pred.collect() #collect all predictions
results = test_data.join(pred)
rmse_differences = results.map(lambda x:(abs(x[1][0] - x[1][1]))**2).mean()
rmse_differences1 = results.map(lambda x:(abs(x[1][0] - x[1][1]))**2).collect() #calculate ranges
rmse = math.sqrt(rmse_differences) 
range1 = []
range2 = []
range3 = []
range4 = []
range5 = []
for diff in rmse_differences1: 
    if diff >= 0 and diff <1:
        range1.append(diff)
    elif diff >= 1 and diff <2:
        range2.append(diff)
    elif diff >= 2 and diff <3:
        range3.append(diff)
    elif diff >= 3 and diff <4:
        range4.append(diff)
    elif diff >= 4:
        range5.append(diff)
r1 = len(range1)
r2 = len(range2)
r3 = len(range3)
r4 = len(range4)
r5 = len(range5)
rmse = math.sqrt(rmse_differences)
with open(output_file,'w') as outfile: #output into file
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
end_time = datetime.datetime.now()
difference1 = end_time - start_time
difference_duration1 = round(difference1.total_seconds(),2)
print('RMSE: ' + str(rmse))
print('>=0 and <1: ' + str(r1) + '\n' + '>=1 and <2: ' + str(r2) + '\n' + '>=2 and <3: ' + str(r3) + '\n' + '>=3 and <4: ' + str(r4) + '\n' + '>=4: ' + str(r5))
print('Duration: ' + str(difference_duration1) +' seconds')
