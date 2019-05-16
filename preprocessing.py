'''
Description:
Transforms data from yahoo into correct format for CF training
- skewness and numbers to undersample by found in R code preproccessing.R
- undersamples training data, does not for validation data

Inputs: training and validation data
outputs: csv in format "<ItemID>,<Rating>,<UserID>" for training and validation set

'''
import sys
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark import SparkContext

def map_ratings(row):
  x = {}
  x['user_id'] = row.user_id
  x['item_id'] = row.item_id
  if row.rating <= 20:
    x['rating'] = 1
  elif row.rating <= 40:
    x['rating'] = 2
  elif row.rating <= 60:
    x['rating'] = 3
  elif row.rating <= 80:
    x['rating'] = 4
  else:
    x['rating'] = 5
  return x

def preprocess_file(input_file_name, spark, sample=True):
    ratings = []
    with open(input_file_name, "r") as fp:
      line = fp.readline()
      while line:
        (user_id,num_ratings) = line.split("|")
        num_ratings = int(num_ratings)
        for i in range(0,num_ratings):
          line = fp.readline()
          (item_id, rating, time, time2) = line.split()
          ratings.append((int(user_id), int(item_id), int(rating)))
        line = fp.readline()
    df = spark.createDataFrame(ratings, 
            ["user_id","item_id","rating"])
    # skewness
    # skewness = df.agg(f.skewness("rating"))
    # skewness.show()
    rdd1 = df.rdd.map(map_ratings)
    df2 = spark.createDataFrame(rdd1)
    if sample is True:
      sampled = df2.sampleBy(
              "rating", 
              fractions={
                  1: 0.334, 
                  2: 1, 
                  3: 0.5930, 
                  4: 0.30258, 
                  5: 0.0899145}, 
              seed=0)
      # skewness
      skewness = sampled.agg(f.skewness("rating"))
      skewness.show()
      #+--------------------+
      #|    skewness(rating)|
      #+--------------------+
      #|-8.24123249452558E-5|
      return sampled
    else:
      return df2

if __name__ == '__main__':
    sc = SparkContext("local", "preprocess_file")
    spark = SparkSession(sc)
    sampled_data = preprocess_file(sys.args[1], spark)
    sampled_data.write.csv(sys.args[2])
    validation_data = preprocess_file(sys.args[3], spark, False)
    validation_data.write.csv(sys.args[4])


