'''
    Batch inference with Ray

    Task: For each book, predict its category using the 
    pretrained NLP model BERT-base
'''
import ray
from pyarrow import dataset
import sys
from typing import Dict
import pandas as pd
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

# Initialize Ray
ray.init(address='auto')

# Redirect stdout, stderr to corresponding files 
sys.stdout = open("bert_ray.out", "w")


dataset_path = 'hdfs://gutasVM:54310/books_dataset/part-1.parquet'

start = time.time()

# Load dataset into a Ray Dataset(single parquet with 92374 books)
ds = ray.data.read_parquet(dataset_path, \
        columns = ['title', 'description', 'language'], \
        filter=(dataset.field('description') != '') & (dataset.field('language')=='eng'), \
        parallelism=-1) \

# Predictor class that uses the pretrained NLP model
class BERTPredictor():
    def __init__(self):
        from transformers import pipeline
        self.model = pipeline(model="bert-base-uncased")
        
    
    def __call__(self, batch: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        batch['description'] = batch['description'].apply(lambda x: " The book is about [MASK]." + x)
        tokenizer_kwargs = {'truncation':True, 'max_length':512} # Truncates the description tokens
        predictions = self.model(list(batch['description']), targets=categories, tokenizer_kwargs=tokenizer_kwargs)
        batch["category"] = [sequences[0]["token_str"] for sequences in predictions]
        return batch

# Distribute the inference in batches. Save the results in hdfs
predictions = ds.map_batches(BERTPredictor, batch_format='pandas', concurrency=10)\
                .drop_columns(['description'])\
                .write_csv("hdfs://gutasVM:54310/bert_results_ray")

end = time.time()

print("Total time: ", (end-start))
