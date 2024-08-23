'''
 Simple dataset exploration using Ray

 Queries:
 1. Find the distribution of languages
 2. For each author, find the average text reviews and the max number of registered pages
 3. Find the top 20 books with most pages that include the word 'book' in their description
'''

import ray
import sys
from ray.data.aggregate import Sum, Max, Count, Mean
from pyarrow import dataset
import time
import warnings


# Redirect stdout, stderr to corresponding files 
sys.stdout = open("dataset_analytics_ray.out", "w")

# Connect to an existing local cluster
ray.init(address='auto')

'''
  Load parquet files from HDFS directory into a Spark DataFrame
  and adjust column types

  The number of files under this directory changed
  during development in order to test the scalability
'''
dataset_path = 'hdfs://gutasVM:54310/books_dataset'

start = time.time()
init_start = start

# Query-1 : Find the distribution of languages      
ds = ray.data.read_parquet(dataset_path, \
                            columns = ['id', 'language'], \
                            filter=(dataset.field('language') != '')\
                          )

lang_ds = ds.groupby('language') \
            .count()

print("Distribution of languages:")
x = lang_ds.take_all()
end = time.time()
print("Total time for query 1: ", (end-start), " sec \n\n")


# Query 2 - For each author, find the average text reviews and the max number of registered pages. Display the top 20
start = time.time()
ds = ray.data.read_parquet(dataset_path,\
                            columns = ['author_name', 'text_reviews_count', 'num_pages'],\
                            filter = (dataset.field('num_pages').is_valid()) & (dataset.field('text_reviews_count').is_valid())\
                            )

authors_ds = ds.groupby( 'author_name') \
                .aggregate(Max('num_pages', alias_name='max_pages'), \
                           Mean('text_reviews_count', alias_name='avg_reviews')) # ignores null values by default


print("Authors: ")
x = authors_ds.take_all()
end = time.time()
print("Total time for query 2: ", (end-start), " sec\n\n")



# Query 3: Find the top 20 books with the most pages that have the word 'book' on their description
start = time.time()
ds = ray.data.read_parquet( dataset_path,  \
                            parallelism = 10000, \
                            columns = ['language','title', 'description', 'num_pages'], \
                            filter=dataset.field('num_pages').is_valid() & (dataset.field('language') == 'eng') \
                          )

descr_ds = ds.filter(lambda book: "book" in book['description']) \
            .sort('num_pages', descending=True)

print("Books with word 'book' in description :")
descr_ds = descr_ds.take(20)
end = time.time()
print("Total time for query 3: ", (end-start), " sec\n\n")
print(descr_ds)


print("Total time for 4 queries: ", (end - init_start), " sec")
