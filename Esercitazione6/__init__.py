__author__ = "Francesco Benetello"

from pyspark import SparkContext, SparkConf
from pyspark.mllib.util import MLUtils
from pyspark.mllib.fpm import FPGrowth
import pyspark

sc = SparkContext()
#tweet = sc.textFile("/srv/2015-01-08_geo_en_it_10M.plain.json")
tweet = sc.textFile("venezia72_2015.txt")
transactions = tweet.map(lambda line: line.strip().split(' '))
unique = transactions.map(lambda x: list(set(x))).cache()
model = FPGrowth.train(unique, minSupport=0.1, numPartitions=16)

result = model.freqItemsets().collect()
for fi in result:
    print(fi)