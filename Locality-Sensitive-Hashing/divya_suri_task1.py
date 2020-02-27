from pyspark import SparkContext 
import random
import itertools as it
import datetime
import sys

def UsertoRowMapping(business,user_dict): #convert string ID of user to integer based on row number in characteristic matrix
    rated_users = business[1]
    business_id = business[0]
    new_list = [] #create new list with row numbers for users
    for user in rated_users:
        user2 = user_dict[user]
        new_list.append(user2)
    return (business_id,new_list)

def HashParameters(n,max_row_number): #generate all hash parameters
    params = []
    k = 0 
    while k < n: #only generate parameters for max number of hash functions
        a = random.randint(0,1000)
        b = random.randint(0,1000)
        params.append([a,b,max_row_number])
        k+=1
    return params

def GenerateHashFunctions(business,hparams,max_row_number): #apply hash parameters in hash functions and then on user_ids
    row_values = []
    for parameters in hparams:
        a = parameters[0]
        b = parameters[1]
        c = parameters[2]
        business_id = business[0]
        initial_row_numbers = business[1]
        new_row_numbers = []
        for y in initial_row_numbers:
            new_number = ((a*y) + b) % max_row_number
            new_row_numbers.append(new_number)
        min_value = min(new_row_numbers)
        row_values.append(min_value)
    return (business[0],row_values)

def CreateBands(business,rows,n): #subset signature matrix into sets of size band
    users = business[1]
    business_id = business[0]
    band_size = []
    p = 0 
    while p <= (n - 1):
        a = users[p:p+rows]
        band_size.append(a)
        p = p + rows
    return (business_id,users, band_size)

def BandManipulation(business): #convert band into key to group later
	users = tuple(business[2])
	business_sig = business[1]
	business_id = business[0]
	return (users,(business_id,business_sig))

def RemoveSigMat(candidate): #to clean up data, remove signature matrix and keep business_id
    cands = []
    cand = candidate[1]
    for c in cand: 
        business_id = c[0]
        cands.append(business_id)
    return cands

def Transform(band,business_dict): #clean up data and add original user list to each business pair
    if len(band) == 2:
        pair1 = band[0]
        pair1_data = business_dict[pair1]
        pair2 = band[1]
        pair2_data = business_dict[pair2]
        return ((pair1,pair1_data),(pair2,pair2_data))
    else:
        combos = list(it.combinations(band,2))
        combos_list = []
        for combo in combos:
            pair1 = combo[0]
            pair1_data = business_dict[pair1]
            pair2 = combo[1]
            pair2_data = business_dict[pair2]
            combos_list.append(((pair1,pair1_data),(pair2,pair2_data)))
        return combos_list

def Jaccard(): #implements jaccard similarity 
	train_RDD = lsh.textFile(input_file)
	header = train_RDD.first()
	train_RDD = train_RDD.filter(lambda x: x!= header) #remove header
	train_initial = train_RDD.map(lambda x:x.split(','))
	user_as_key = train_initial.map(lambda x:(x[0],x[1]))
	business_as_key = train_initial.map(lambda x:(x[1],x[0]))
	user_combos = user_as_key.groupByKey().map(lambda x : (x[0], list(x[1]))).zipWithIndex().map(lambda x: (x[0][0],x[1])) #add row numbers to transform string user_id to number
	users_row_numbers = user_combos.collect()
	user_dict = {} #to map user_id to integer in the future
	for user in users_row_numbers: 
	    user_dict[user[0]] = user[1]
	business_combos = business_as_key.groupByKey().map(lambda x : (x[0], list(x[1]))) #group businesses to generate business as key and list of users who rated it as values
	business_combos_with_rows = business_combos.map(lambda x: UsertoRowMapping(x,user_dict)) #transform values to integers
	bus_comb = business_combos_with_rows.collect()
	business_dict = {} 
	for bus in bus_comb: 
	    business_dict[bus[0]] = bus[1]
	max_row_number = len(users_row_numbers) #number of bins
	n = 60 #number of hash functions
	bands = 20
	rows = 3
	hparams = HashParameters(n,max_row_number) 
	sig_mat = business_combos_with_rows.map(lambda x: GenerateHashFunctions(x, hparams,max_row_number))
	banded_sig_mat = sig_mat.map(lambda x: CreateBands(x,rows,n))

	b1 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][0])) #extract all bands into one rdd for that subset
	b2 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][1]))
	b3 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][2]))
	b4 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][3]))
	b5 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][4]))
	b6 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][5]))
	b7 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][6]))
	b8 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][7]))
	b9 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][8]))
	b10 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][9]))
	b11 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][10]))
	b12 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][11]))
	b13 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][12]))
	b14 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][13]))
	b15 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][14]))
	b16 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][15]))
	b17 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][16]))
	b18 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][17]))
	b19 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][18]))
	b20 = banded_sig_mat.map(lambda x:(x[0],x[1],x[2][19]))

	b1_amended = b1.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0], list(x[1]))).filter(lambda x:len(x[1])>=2) #group by key and filter out if no businesses align for that band
	b2_amended = b2.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0], list(x[1]))).filter(lambda x:len(x[1])>=2)
	b3_amended = b3.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0], list(x[1]))).filter(lambda x:len(x[1])>=2)
	b4_amended = b4.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0], list(x[1]))).filter(lambda x:len(x[1])>=2)
	b5_amended = b5.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0], list(x[1]))).filter(lambda x:len(x[1])>=2)
	b6_amended = b6.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0], list(x[1]))).filter(lambda x:len(x[1])>=2)
	b7_amended = b7.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0], list(x[1]))).filter(lambda x:len(x[1])>=2)
	b8_amended = b8.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0], list(x[1]))).filter(lambda x:len(x[1])>=2)
	b9_amended = b9.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0], list(x[1]))).filter(lambda x:len(x[1])>=2)
	b10_amended = b10.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x:len(x[1])>=2)
	b11_amended = b11.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x:len(x[1])>=2)
	b12_amended = b12.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x:len(x[1])>=2)
	b13_amended = b13.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x:len(x[1])>=2)
	b14_amended = b14.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x:len(x[1])>=2)
	b15_amended = b15.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x:len(x[1])>=2)
	b16_amended = b16.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x:len(x[1])>=2)
	b17_amended = b17.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x:len(x[1])>=2)
	b18_amended = b18.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x:len(x[1])>=2)
	b19_amended = b19.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x:len(x[1])>=2)
	b20_amended = b20.map(lambda x:BandManipulation(x)).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x:len(x[1])>=2)


	candidates = b1_amended.union(b2_amended) #add all the candidate pairs together
	candidates = candidates.union(b3_amended)
	candidates = candidates.union(b4_amended)
	candidates = candidates.union(b5_amended)
	candidates = candidates.union(b6_amended)
	candidates = candidates.union(b7_amended)
	candidates = candidates.union(b8_amended)
	candidates = candidates.union(b9_amended)
	candidates = candidates.union(b10_amended)
	candidates = candidates.union(b11_amended)
	candidates = candidates.union(b12_amended)
	candidates = candidates.union(b13_amended)
	candidates = candidates.union(b14_amended)
	candidates = candidates.union(b15_amended)
	candidates = candidates.union(b16_amended)
	candidates = candidates.union(b17_amended)
	candidates = candidates.union(b18_amended)
	candidates = candidates.union(b19_amended)
	candidates = candidates.union(b20_amended)

	candidates_cleaned = candidates.map(lambda x: RemoveSigMat(x)) #remove signature matrix
	candidates_original = candidates_cleaned.map(lambda x: Transform(x,business_dict)) #add original user_id
	candidate_sim = candidates_original.collect()

	jac_sim = [] #to get jaccard similarities of all the pairs
	for cand in candidate_sim:
	    if isinstance(cand,tuple):
	        pair1 = cand[0][0]
	        pair1_data = set(cand[0][1])
	        pair2 = cand[1][0]
	        pair2_data = set(cand[1][1])
	        jac = ((len(pair1_data&pair2_data)))/float(len(pair1_data|pair2_data))
	        inp = ((pair1,pair2),jac)
	        jac_sim.append(inp)
	    else:
	        for combo in cand: 
	            pair1 = combo[0][0]
	            pair1_data = set(combo[0][1])
	            pair2 = combo[1][0]
	            pair2_data = set(combo[1][1])
	            jac = ((len(pair1_data&pair2_data)))/float(len(pair1_data|pair2_data))
	            inp = ((pair1,pair2),jac)
	            jac_sim.append(inp)

	similar = {} #filter out those with jaccard less than 0.5
	for pair in jac_sim:
	    a = pair[0][0]
	    b = pair[0][1]
	    jaccard = pair[1]
	    if(jaccard>=0.5):
	        if(a<b):
	            if (a,b) not in similar:
	                similar[(a,b)] = jaccard       
	        else:
	            if (b,a) not in similar:
	                similar[(b,a)] = jaccard 

	similar_pairs2 = [] #for output file
	for k,v in similar.items():
	    inp = (k,v)
	    similar_pairs2.append(inp)
	sorted_similar_pair2 = sorted(similar_pairs2, key=lambda x: (x[0][0], x[0][1]))

	with open(output_file,'w') as outfile:
		outfile.write('business_id_1')
		outfile.write(",")
		outfile.write('business_id_2')
		outfile.write(",")
		outfile.write("similarity")
		outfile.write('\n')
		for p in sorted_similar_pair2:
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
	print('Duration: ' + str(difference_duration1) +' seconds')

if __name__ == "__main__":
	start_time = datetime.datetime.now()
	input_file = sys.argv[1]
	method = str(sys.argv[2])
	output_file = sys.argv[3]
	lsh = SparkContext.getOrCreate()
	if method == 'jaccard':
		Jaccard()
