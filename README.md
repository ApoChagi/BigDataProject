# BigDataProject
Big Data Management and Information Systems

Team members:
- Chatzianagnostou Apostolos (el19021)
- Tsikriteas Panagiotis (el19868)
- Magkoutas Andreas (el19015)

## About the project
### Comparison between Python scaling frameworks for big data analysis and ML : Spark vs Ray 
The project is about testing the scalability of two major Python frameworks: Apache Spark and Ray especially in big data analysis and Machine Learning (ML)
<br> <br>
Scripts :
+ Simple analysis on book dataset (queries)
+ XGBoost RandomForest Regressor training & predictions
+ Batch inference on BERT-base pretrained model (NLP) 

## Prerequisites

In order to run the scripts, you need the following prerequisites:
+ A cluster of at least 2 nodes with HDFS and Spark over YARN. An instruction guide for a 2 node cluster can be found on [this](https://colab.research.google.com/drive/1pjf3Q6T-Ak2gXzbgoPpvMdfOHd1GqHZG?usp=sharing) notebook. Make sure to adjust some parameters during installation in order to match your cluster resources
+ Python-3.10.12 (on each node)
+ [Ray](https://www.ray.io/) can be installed on each node by simply running ``` pip install -U ray```

## Running the scripts
You can find a guide about how to run the scripts in each sub-directory
