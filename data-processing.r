import re
import sys

from pyspark.sql import SparkSession

input_dir = './data/'

input_artists = paste(path, "/artistData1.txt", sep="")
input_albums = paste(path, "/albumData1.txt", sep="")
input_genres = paste(path, "/genreData1.txt", sep="")
input_tracks = paste(path, "/trackData1.txt", sep="")
input_stats = paste(path, "/stats1.txt", sep="")
input_train_file = input_dir + 'trainIdx1.txt'



songs <- read.table(input_train_file, sep=",", 
                     header=FALSE, 
                     comment.char = "",
                     quote="", fill=FALSE)




