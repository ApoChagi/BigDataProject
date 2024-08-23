import pandas as pd
from transformers import pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
import sys
import time

categories = [  'fantasy', 'history', 'science',     \
                'poetry', 'romance', 'adventure',    \
                'crime', 'poetry', 'humor',          \
                'thriller', 'mystery', 'religion',   \
                'horror', 'biography', 'travel',     \
                'cook', 'politics', 'technology',    \
                'art', 'sports', 'education',        \
                'fitness', 'western', 'other'        \
            ]

# Single file with 92374 books
dataset_path = 'hdfs://gutasVM:54310/books_dataset/part-1.parquet'

# Spark context initialization
spark = SparkSession.builder.appName('Batch inference using BERT').getOrCreate()

# Redirect stdout, stderr to corresponding files 
sys.stdout = open("bert_spark.out", "w")


start = time.time()

# Load and preprocess the dataset
ds = spark.read.parquet(dataset_path, header=True)
ds = ds.filter((col("language") == "eng") & col("title").isNotNull() & (col("description") != '')) \
        .select("title", "description") \
        .limit(1000)    # limit the number of books due to poor performance


# Set the pretrained model
pipe = pipeline(model="bert-base-uncased")
prompt = "The book is about [MASK]."

# pandas_udf function that uses the pretrained BERT model for prediction
@pandas_udf('string')
def get_category(descriptions: pd.Series) -> pd.Series:
    tokenizer_kwargs = {'truncation':True, 'max_length':512} # Truncates the description tokens
    result = []
    for descr in descriptions:
        output = pipe(prompt+descr, targets = categories, tokenizer_kwargs=tokenizer_kwargs)
        result.append(output[0]['token_str'])
    return pd.Series(result)


# Perform inference and save results in hdfs
ds.select('title', get_category(ds.description).alias('category'))\
        .write.mode("overwrite").csv("hdfs://gutasVM:54310/bert_results")

end = time.time()

print("Total time: ", (end-start))

