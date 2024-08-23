'''
      Simple dataset exploration using Spark

      Queries:
      1. Find the distribution of languages
      2. For each author, find the average text reviews and the max number of registered pages
      3. Find the top 20 books with most pages that include the word 'book' in their description
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, count, desc
import sys
import time


sys.stdout = open("dataset_analysis_spark.out", "w")


spark = SparkSession.builder.appName("Dataset analysis with SPARK").getOrCreate()

'''
      Load parquet files from HDFS directory into a Spark DataFrame
      and adjust column types

      The number of files under this directory changed
      during development in order to test the scalability
'''
dataset_path = "hdfs://gutasVM:54310/books_dataset"
start = time.time()
init_start = start

# Create dataframe with inferred schema
df = spark.read.parquet(dataset_path, header=True)

# Adjust data types of specific columns
books_df = df.withColumn("publication_year", col("publication_year").cast("Integer")) \
             .withColumn("average_rating", col("average_rating").cast("Double")) \
             .withColumn("text_reviews_count", col("text_reviews_count").cast("Double")) \
             .withColumn("ratings_count", col("ratings_count").cast("Double")) \
             .withColumn("num_pages", col("num_pages").cast("Double")) \
             .filter((col("average_rating").isNotNull()) & (col("text_reviews_count").isNotNull()) \
                   & (col("ratings_count").isNotNull()) & (col("num_pages").isNotNull()) \
                   & (col("title").isNotNull())& (col("author_name").isNotNull()))


#Query1 = Find the distribution of languages
Q1 = books_df.select("language", "id") \
         .filter(col("language") != "")\
	     .groupBy(col("language")) \
	     .agg(count("id")) \


print("Distribution of languages: ")
print(Q1.collect()) # triggers the whole computation
end = time.time()
print("Time taken for query 1: ", (end-start), " sec")


#Query 2: For each author, find the average text reviews and the max number of registered pages
start = time.time()
Q2 = books_df.select("author_name", "text_reviews_count", "num_pages") \
	     .groupBy(col("author_name")) \
	     .agg(avg("text_reviews_count").alias("average_reviews"), max("num_pages").alias("max_pages")) \


print("Authors: ")
_ = Q2.collect() # triggers the whole computation
end = time.time()
print("Time taken for query 2: ", (end-start), " sec")


# Query 3: Find the top 20 books with the most pages that have the word 'book' on their description
start = time.time()
Q3 = books_df.select("title", "description", "num_pages") \
            .filter(col("description").like("%book%") & (col("language") == "eng")) \
            .orderBy(col("num_pages").desc())

print("Books with word 'book' in description :")
Q3.show()   # triggers the whole computation
end = time.time()
print("Time taken for query 3: ", (end-start), " sec")

print("Total time for 4 queries: ", (end - init_start), " sec")
