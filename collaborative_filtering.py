'''
Description:

takes train and validation files prepared in preprocessing.py and uses ALS model to recommend 10 songs for each user
- model is fitted using least RMSE from tune_ALS function before hand
- params for model: maxIter=20, regParam=, rank=
'''

import sys
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS


input_train_file = sys.args[1]
input_validation_file = sys.args[2]
spark = SparkSession.builder \
  .appName("music Collab filtering") \
  .getOrCreate()

# Structure of data post- preprocessing
schema = StructType([
    StructField("item_id", IntegerType()),
    StructField("rating", IntegerType()),
    StructField("user_id", IntegerType())
])

# Load in training and test data
training = spark.read.schema(schema)\
  .option("header", "false")\
  .option("mode", "DROPMALFORMED")\
  .csv(input_train_file)
test = spark.read.schema(schema)\
  .option("header", "false")\
  .option("mode", "DROPMALFORMED")\
  .csv(input_validation_file)

def tune_ALS(train_data, validation_data, maxIter, regParams, ranks):
  min_error = float('inf')
  best_rank = -1
  best_regularization = 0
  best_model = None
  for rank in ranks:
    for reg in regParams:
      als = ALS(maxIter=maxIter, regParam=reg, rank=rank, userCol="user_id", itemCol="item_id", ratingCol="rating", coldStartStrategy="drop")
      model = als.fit(train_data)
      predictions = model.transform(validation_data)
      evaluator = RegressionEvaluator(metricName="rmse",
        labelCol="rating",
        predictionCol="prediction")
      rmse = evaluator.evaluate(predictions)
      print('{} latent factors and regularization = {}: '
                  'validation RMSE is {}'.format(rank, reg, rmse))
      if rmse < min_error:
        min_error = rmse
        best_rank = rank
        best_regularization = reg
        best_model = model
  print('\nThe best model has {} latent factors and '
    'regularization = {}'.format(best_rank, best_regularization))
  return best_model
# Find best model tuning (only needed to do once)
# model = tune_ALS(training, test, 20, [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09], range(1,20))
als = ALS(maxIter=20, regParam=0.09, rank=11, userCol="user_id", itemCol="item_id", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(train_data)

# Find top reccomended items for each user
userRecs = model.recommendForAllUsers(10)

# save recommendations to file
userRecs.write.csv(sys.args[3], header=True, mode="overwrite")
