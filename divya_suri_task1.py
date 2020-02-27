from pyspark import SparkContext 
import itertools as it 
import datetime
import sys 
import numpy as np 

def FindFrequentItems_Case1(baskets,support,num_of_partitions):
	lengths_case1 = []
	for bus in baskets:
		item = (bus[1],(len(bus[1])+1))
		lengths_case1.append(item)
	all_basket_combos_case1 = []
	for x in lengths_case1:
		for i in range(1,x[1]):
			for y in list(it.combinations(x[0],i)):
				list_input = (y,1)
				all_basket_combos_case1.append(list_input)
	local_frequencies = {}
	for combo in all_basket_combos_case1:
		if combo[0] not in local_frequencies:
			local_frequencies[combo[0]] = 1
		else:
			local_frequencies[combo[0]] += 1
	frequents = []
	for k,v in local_frequencies.items():
		if v >= (support/num_of_partitions):
			frequents.append(k)
	return sorted(frequents,key=len,)

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
			singletons.append(val)
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


def FindFrequentItems_Case2(baskets):
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
	case = sys.argv[1]
	support = int(sys.argv[2])
	output_file = sys.argv[4]

	SON = SparkContext.getOrCreate()

	if case == '1':
		small2_RDD = SON.textFile(input_file)
		header = small2_RDD.first()
		small2_RDD = small2_RDD.filter(lambda x: x!= header)
		initial = small2_RDD.map(lambda x:x.split(','))
		initial_amend = initial.map(lambda x:(x[0],x[1]))
		mapped_initial_amend = initial_amend.map(lambda x: (x,1))
		reduced_initial_amend = mapped_initial_amend.reduceByKey(lambda x,y:x+y)
		initial_combos_case1 = reduced_initial_amend.map(lambda x:(x[0][0],x[0][1])) 
		final_combos_case1 = initial_combos_case1.groupByKey().map(lambda x : (x[0], list(x[1])))
		num_of_partitions = final_combos_case1.getNumPartitions()
		case1_baskets = final_combos_case1.values().collect()
		frequents = final_combos_case1.mapPartitions(lambda x:(FindFrequentItems_Case1(x,support,num_of_partitions)))
		collected_freq = frequents.collect()
		np_candidates = np.array(collected_freq)
		unique_candidates = list(np.unique(np_candidates))
		unique_candidates = sorted(unique_candidates,key=len)
		itemsets = []
		total_frequencies = {}
		for basket in case1_baskets:
			for candidate in unique_candidates:
				if set(candidate).issubset(set(basket)):
					if candidate not in total_frequencies:
						total_frequencies[candidate] = 1
					else:
						total_frequencies[candidate] += 1
			for freq in total_frequencies:
				if total_frequencies[freq] >= support:
					itemsets.append(freq)
		np_itemsets = np.array(itemsets)
		unique_itemsets = list(np.unique(np_itemsets))
		unique_itemsets = sorted(unique_itemsets,key=len)
		candidate_groups = []
		for k,g in it.groupby(unique_candidates, len):
			candidate_groups.append(list(g))
		candidate_groups2 = []
		for group in candidate_groups:
			group = sorted(group)
			candidate_groups2.append(group)
		unique_itemsets_groups = []
		for x,y in it.groupby(unique_itemsets,len):
			unique_itemsets_groups.append(list(y))
		unique_itemsets_groups2 = []
		for group in unique_itemsets_groups:
			group = sorted(group)
			unique_itemsets_groups2.append(group)
		with open(output_file,'w') as outfile:
			outfile.write('Candidates:')
			outfile.write('\n')
			outfile.write('\n')
			for group in candidate_groups2:
				for item in group:
					if len(item) == 1:
						item = item[0]
						outfile.write("('" + item + "')" +',')
					else:
						outfile.write(str(item) + ',')
				outfile.write('\n')
				outfile.write('\n')
			outfile.write('Frequent Itemsets:')
			outfile.write('\n')
			outfile.write('\n')
			for group in unique_itemsets_groups2:
				for item in group:
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


	if case == '2':
		small2_RDD = SON.textFile(input_file)
		header = small2_RDD.first()
		small2_RDD = small2_RDD.filter(lambda x: x!= header)
		initial = small2_RDD.map(lambda x:x.split(','))
		initial_amend = initial.map(lambda x:(x[0],x[1]))
		mapped_initial_amend = initial_amend.map(lambda x: (x,1))
		reduced_initial_amend = mapped_initial_amend.reduceByKey(lambda x,y:x+y)
		initial_combos_case2 = reduced_initial_amend.map(lambda x:(x[0][1],x[0][0]))
		final_combos_case2 = initial_combos_case2.groupByKey().map(lambda x : (x[0], list(x[1])))
		num_of_partitions = final_combos_case2.getNumPartitions()
		case2_baskets = final_combos_case2.values().collect()
		frequents = final_combos_case2.mapPartitions(FindFrequentItems_Case2)
		collected_freq = frequents.collect()
		candidates_not_distinct = []
		for group in collected_freq:
			for candidate in group:
				candidates_not_distinct.append(candidate)
		distinct_candidates = list(set([i for i in candidates_not_distinct])) 
		distinct_candidates2 = []
		for candidate in distinct_candidates:
			candidate = tuple(candidate)
			distinct_candidates2.append(candidate)
		distinct_candidates2.sort(key=lambda item: (len(item), item))
		itemsets = []
		total_frequencies = {}
		for basket in case2_baskets:
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
			for item in distinct_candidates2:
				if len(item) == 1:
					item = item[0]
					outfile.write("('" + item + "')" +',')
				else:
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
	

