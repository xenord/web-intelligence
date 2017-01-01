# coding=utf-8
__author__ = "Francesco Benetello"

from pyspark import SparkContext, SparkConf
from pyspark.mllib.util import MLUtils
from pyspark.mllib.fpm import FPGrowth
import pyspark

sc = SparkContext()
tweet = sc.textFile("2015-01-08_geo_en_it_10M.plain.json")
#tweet = sc.textFile("venezia72_2015.txt")
transactions = tweet.map(lambda line: line.strip().split(' '))
model = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)
result = model.freqItemsets().collect()
for fi in result:
    print(fi)