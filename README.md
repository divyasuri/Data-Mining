# Data Mining Projects
Examples of data mining projects I have completed, including finding frequent itemsets and collaborative filtering recommendation systems. 

1. Finding Frequent Itemsets

For this project, I utilized Spark and Python to implement the SON algorithm that would find frequent itemsets of any size in a Yelp dataset of users and businesses they have reviewed. The Limited Pass Algorithm used is the Apriori algorithm. The execution of this file is done in this format: 

spark-submit FindingFrequentItemsets.py <case number> <support> <input_file_path> <output_file_path>
  
where case number is either 1 or 2 (1 = frequent businesses, 2 = frequent users), input file should be a CSV and output file should be a TXT file which shows the candidates from the SON algorithm and the true frequent itemsets. 

2. User & Model Based Collaborative Filtering 

For this project, I utilized Spark and Python to build a recommendation system that would predict Yelp ratings for given user_ids and business_ids. In both cases, I was able to achieve a RMSE score of less than 1.8. The execution of this file is done in this format: 

spark-submit User&ModelBasedCF.py <train_file_path> <test_file_path> <case_id> <output_file_name>

where train_file_path is a CSV to train the system, test_file_path is a CSV to test the system, case_id is 1 or 2 (1 = Model Based CF, 2 = User Based CF) and output_file_name should be a CSV. 

3. Finding Similar Users using Jaccard Similarity and Locality Sensitive Hashing 

For this project, I utilized Spark and Python to implement the LSH technique to identify similar businesses based on ratings given by users. For this, I used Jaccard similarity of greater than 0.5 as the metric of high similarity. My program was able to achieve a precision of 1 and recall of 0.95. The execution of this file is done in this format:

spark-submit LSH.py <input_file_path> jaccard <output_file_path> 

where the input file should be a CSV with users, businesses and their ratings for that business and output file should be a CSV.
