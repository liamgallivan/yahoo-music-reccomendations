from pyspark.sql import functions as f
'''
path = '~/code/yahoo-music-recommendations/data'

input_artists = paste(path, "/artistData1.txt", sep="")
input_albums = paste(path, "/albumData1.txt", sep="")
input_genres = paste(path, "/genreData1.txt", sep="")
input_tracks = paste(path, "/trackData1.txt", sep="")
input_stats = paste(path, "/stats1.txt", sep="")
input_train_file = paste(path, "/testIdx1.txt", sep="")
'''

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

def preprocess_file(input_file_name):
    ratings = []

    with open(input_file_file, "r") as fp:
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
    n = 300000

    rdd1 = df.rdd.map(map_ratings)
    df2 = spark.createDataFrame(rdd1)
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
    # skewness = sampled.agg(f.skewness("rating"))
    # skewness.show()
    return sampled

if __name__ == '__main__':
    pass
