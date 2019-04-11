import re
import sys

from pyspark.sql import SparkSession

input_dir = './data/'

input_artists = input_dir + 'artistData1.txt'
input_albums = input_dir + 'albumData1.txt'
input_genres = input_dir + 'genreData1.txt'
input_tracks = input_dir + 'trackData1.txt'
input_stats = input_dir + 'stats1.txt'
input_train_file = input_dir + 'validationIdx1.txt'


