# yahoo-music-reccomendations
Data mining project using anonymous user rating data from Yahoo competition data

## File Descriptions

### data-processing.r
Used to find initial skewness and distribution of ratings
- primarily for data exploration

### preprocessing.py
Used to preprocess raw data files

To Run:
`spark-submit preprocessing.py <train_file> <train_output_filename> <validation_file> <validation_output_filename>`
Then move the resulting files out of the directories spark created and use in:

### collaborative_filtering.py
Main code used for training and getting predictions from ALS model

To Run:
`spark-submit collaborative_filtering.py <train_file> <validation_file> <output_file>`
- train file and validation file are output from preprocessing.py

## Non-code files

### tuning.txt
results of tuning function

### data-format.txt
format of raw data


## Data
Found in Yahoo Music Recommendation challenge. (KDD cup 2011)
[Link](https://webscope.sandbox.yahoo.com/catalog.php?datatype=c)


## Results
Results available on request
