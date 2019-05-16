path = '~/Desktop/Webscope_C15/ydata-ymusic-kddcup-2011-track1'

input_artists = paste(path, "/artistData1.txt", sep="")
input_albums = paste(path, "/albumData1.txt", sep="")
input_genres = paste(path, "/genreData1.txt", sep="")
input_tracks = paste(path, "/trackData1.txt", sep="")
input_stats = paste(path, "/stats1.txt", sep="")
input_train_file = paste(path, "/testIdx1.txt", sep="")


#[(u1,s1,r), (u2,s2,r)]
#u1,[(s1,r),(s2,3),(s10,r)] -> get all the songs that user 1 has rated 
#u2,[...]
#sim()


#s2[(u1,r),(u10,r)...]

songs <- read.table(input_train_file, sep="", 
                     header=FALSE, 
                     comment.char = "",
                     quote="", fill=TRUE)

x <- c("item", "rating", "time")
colnames(songs) = x

summary(songs)

hist(songs$rating)

library(e1071)

myclass = songs$rating
skewness(myclass,na.rm = TRUE,type=2)

hist(songs$rating)

#install.packages("ggpubr")
library(ggpubr)

songs_by_item <- group_by(songs, rating)
head(songs_by_item)

#Clean the NA values -> actually remove them from the dataset
#precision

n = 300000
sample(1:nrow(songs[songs$rating <= 20]),n)
sample(1:nrow(songs))
