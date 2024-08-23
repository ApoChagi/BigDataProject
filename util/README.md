## Utility scripts
These scripts were used to adjust the dataset before tests. <br><br>
For dataset analysis and NLP with BERT, we used [this](https://www.kaggle.com/datasets/opalskies/large-books-metadata-dataset-50-mill-entries) dataset from kaggle. The initial dataset is an 90GB file in JSON format, with a lot of reduntant information.
*  `json_to_csv` : converts the initial JSON to a csv file and keeps only the necessary columns
*  `csv_to_parquet` : converts the csv file to many parquet
 <br> <br>
  For the XGBOOST task, the book dataset wasn't sufficent, so we decided to create our own dataset:
* `reg_dataset.py` : a simple script that uses scikit-learn library to create a dataset for regression training and save it as csv
