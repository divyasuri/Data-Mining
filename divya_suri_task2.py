from pyspark import SparkContext 
import pandas as pd
import itertools as it
import datetime
import sys

def GetSingles(users,support,num_of_partitions):
	single_frequencies = {}
	for user in users:
		return user
		if user not in single_frequencies:
			single_frequencies[(user)] = 1
		else:
			single_frequencies[(user)] += 1
	singletons = []
	for val in single_frequencies:
		if single_frequencies[val] >= (support/num_of_partitions):
			singletons.append(str(val))
	return sorted(singletons)

def FindFrequent(users,singletons,combinations, support,num_of_partitions):
	int_frequencies = {}
	freq_cur = []
	for combo in combinations:
		for user in users:
			if set(combo).issubset(set(user)):
				if combo not in int_frequencies:
					int_frequencies[combo] = 1
				else:
					int_frequencies[combo] += 1
	for freq in int_frequencies:
		if int_frequencies[freq] >= (support/num_of_partitions):
			freq_cur.append(freq)
	return freq_cur


def FindFrequentItems(baskets):
	users = []
	for user in baskets:
		input_tuple = tuple(user[1])
		users.append(input_tuple)
	candidates = []
	singletons = GetSingles(users,support,num_of_partitions)
	candidates.append(singletons)
	freq_pre = singletons
	n = 2
	while True:
		if len(freq_pre) == 0:
			break 
		else:
			if n == 2:
				possible_combinations = list(it.combinations(singletons,n))
				freq_cur = FindFrequent(users,singletons,possible_combinations,support,num_of_partitions)
				candidates.append(freq_cur)
				freq_pre = freq_cur
				n+=1
			else:
				possible_combinations = []
				for single in singletons:
					for val in freq_pre:
						val_list = list(val)
						if single not in val_list:
							val_list.append(single)
							val_tuple = tuple(val_list)
							possible_combinations.append(val_tuple)
				freq_cur = FindFrequent(users,singletons,possible_combinations,support,num_of_partitions)
				if len(freq_cur) == 0:
					break
				candidates.append(freq_cur)
				freq_pre = freq_cur
				n+=1
	return candidates

if __name__ == "__main__":
	start_time = datetime.datetime.now()
	input_file = sys.argv[3]
	support = int(sys.argv[2])
	filter_threshold = int(sys.argv[1])
	output_file = sys.argv[4]

	SON = SparkContext.getOrCreate()

	data_df = pd.read_csv(input_file)
	data_df = data_df.loc[:, ['TRANSACTION_DT','CUSTOMER_ID','PRODUCT_ID']]
	data_df['CUSTOMER_ID'] = data_df['CUSTOMER_ID'].apply(lambda x:str(x))
	data_df['DATE-CUSTOMER_ID'] = data_df[['TRANSACTION_DT','CUSTOMER_ID']].apply(lambda x:'-'.join(x),axis = 1)
	data_df = data_df.loc[:,['DATE-CUSTOMER_ID','PRODUCT_ID']]
	data_df.to_csv('data_preprocessed.csv',index=False)
	data = SON.textFile('data_preprocessed.csv')
	header = data.first()
	data = data.filter(lambda x: x!= header)
	initial = data.map(lambda x:x.split(','))
	initial_amend = initial.map(lambda x:(x[0],x[1]))
	mapped_initial_amend = initial_amend.map(lambda x: (x,1))
	reduced_initial_amend = mapped_initial_amend.reduceByKey(lambda x,y:x+y)
	data_RDD = reduced_initial_amend.map(lambda x:(x[0][0],x[0][1])) 
	amended_data_RDD = data_RDD.groupByKey().map(lambda x : (x[0], list(x[1])))
	qualified_customers = amended_data_RDD.filter(lambda x:len(x[1]) >= filter_threshold)
	num_of_partitions = qualified_customers.getNumPartitions()
	products = qualified_customers.values().collect()
	frequents = qualified_customers.mapPartitions(FindFrequentItems)
	collected_freq = frequents.collect()
	candidates_not_distinct = []
	for group in collected_freq:
		for candidate in group:
			if len(candidate) ==1:
				candidates_not_distinct.append(str(candidate))
			else:
				candidates_not_distinct.append(candidate)
	distinct_candidates = list(set([i for i in candidates_not_distinct]))
	distinct_candidates.sort(key=len)
	total_frequencies = {}
	itemsets = []
	for basket in products:
		for candidate in distinct_candidates:
			if set(candidate).issubset(set(basket)):
				if candidate not in total_frequencies:
					total_frequencies[candidate] = 1
				else:
					total_frequencies[candidate] += 1
		for freq in total_frequencies:
			if total_frequencies[freq] >= support:
				itemsets.append(freq)
	distinct_itemsets = list(set([i for i in itemsets]))
	distinct_itemsets2 = []
	for itemset in distinct_itemsets:
		itemset = tuple(itemset)
		distinct_itemsets2.append(itemset)
	distinct_itemsets2.sort(key=lambda item: (len(item), item))
	with open(output_file,'w') as outfile:
		outfile.write('Candidates:')
		outfile.write('\n')
		outfile.write('\n')
		for item in distinct_candidates:
			outfile.write(str(item) + ',')
			outfile.write('\n')
			outfile.write('\n')
		outfile.write('Frequent Itemsets:')
		outfile.write('\n')
		outfile.write('\n')
		for item in distinct_itemsets2:
			if len(item) == 1:
				item = item[0]
				outfile.write("('" + item + "')" +',')
			else:
				outfile.write(str(item) + ',')
			outfile.write('\n')
			outfile.write('\n')
		outfile.close()
	end_time = datetime.datetime.now()
	difference = end_time - start_time
	difference_duration = round(difference.total_seconds(),2)
	print('Duration: ' + str(difference_duration) +' seconds')
	
