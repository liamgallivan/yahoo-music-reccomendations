
path = '~/Desktop/Webscope_C15/ydata-ymusic-kddcup-2011-track1'

input_artists = paste(path, "/artistData1.txt", sep="")
input_albums = paste(path, "/albumData1.txt", sep="")
input_genres = paste(path, "/genreData1.txt", sep="")
input_tracks = paste(path, "/trackData1.txt", sep="")
input_stats = paste(path, "/stats1.txt", sep="")
input_train_file = paste(path, "/testIdx1.txt", sep="")


ratings = []

with open(input_train_file, "r") as fp:
	line = fp.readline()
	(user_id,num_ratings) = line.split("|")
	num_ratings = int(num_ratings)
	for i in range(0,num_ratings)
		line = fp.readline()
		(song_id, rating, time, time2) = line.split()
		ratings.append((int(song_id), int(user_id), int(rating)))


df = sqlcontext.createDataFrame(ratings, ["item_id","user_id","rating"]).collect()








