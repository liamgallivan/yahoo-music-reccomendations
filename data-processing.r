
library(Matrix)

input_dir = '~/code/yahoo-music-recommendations/data'

input_artists = paste(input_dir, "/artistData1.txt", sep="")
input_albums = paste(input_dir, "/albumData1.txt", sep="")
input_genres = paste(input_dir, "/genreData1.txt", sep="")
input_tracks = paste(input_dir, "/trackData1.txt", sep="")
input_stats = paste(input_dir, "/stats1.txt", sep="")
input_train_file = paste(input_dir, '/testIdx1.txt', sep="")

# todo: plot statistics and histograms
#       - ratings 0 - 100

# todo:  fix skewness

# todo: CB model collaborative filtering
#       - generate matrix
conn <- file(input_train_file,open="r")
lines <- readLines(conn)
user_ids <- c()
item_ids <- c()

rating_table <- data.frame(item_id=c(), user_id=c(), rating=c())

i <- 1
while (i < length(lines)  i < 1000) {
  info <- lines[i].strsplit("|")
  user_id <- as.numeric(info[0])
  num_ratings <- as.numeric(info[1])
  ratings_vec <- c()
  item_vec <- c()
  for (current_rating in 1:num_ratings) {
    i <- i + 1
    rating_info <- strsplit(lines[i], "\\s+")
    item_rating <- as.numeric(rating_info[1])
    item_id <- as.numeric(rating_info[0])
    append(ratings_vec, item_rating)
    append(item_vec, item_id)
  }
  
  new_frame = data.frame(item_id=item_vec, user_id=rep(c(user_id), times=num_ratings), rating=ratings_vec)
  rating_table = rbind(rating_table, new_frame)
  i <- i + 1
}

close(conn)