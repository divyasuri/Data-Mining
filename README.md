# Data Mining Projects

Tools: Python (Pandas, NumPy, PySpark), Spark

Examples of data mining projects I have completed, including finding frequent itemsets and collaborative filtering recommendation systems. 

1. Finding Frequent Itemsets

For this project, I utilized Spark and Python to implement the SON algorithm that would find frequent itemsets of any size in a Yelp dataset of users and businesses they have reviewed. The Limited Pass Algorithm used is the Apriori algorithm. The execution of this file is done in this format: 

spark-submit FindingFrequentItemsets.py <case number> <support> <input_file_path> <output_file_path>
  
where case number is either 1 or 2 (1 = frequent businesses, 2 = frequent users), input file should be a CSV and output file should be a TXT file which shows the candidates from the SON algorithm and the true frequent itemsets. 

2. Finding Similar Users using Jaccard Similarity and Locality Sensitive Hashing 

For this project, I utilized Spark and Python to implement the LSH technique to identify similar businesses based on ratings given by users. For this, I used Jaccard similarity of greater than 0.5 as the metric of high similarity. My program was able to achieve a precision of 1 and recall of 0.95. The execution of this file is done in this format:

spark-submit LSH.py <input_file_path> jaccard <output_file_path> 

where the input file should be a CSV with users, businesses and their ratings for that business and output file should be a CSV.

3. Implementing an Item-Based Collaborative Filtering Recommendation System to Predict User Ratings on Unseen Businesses

For this project, I implemented a content-boosted item-based collaborative filtering system using means to impute missing data. I boosted the data for each business with content from a JSON file. I included markers of whether the business was a Beauty Salon, Restaurant, if it was open, its average star rating and whether it was local. In addition, I used adjusted cosine similarity as a similarity metric and normalized each user rating with the average of the user's ratings. I also used a case amplification weight (2.5) to reduce the influence of items with a low similarity. By iteratively working on this project, I ultimately achieved an RMSE of 1.07. 

