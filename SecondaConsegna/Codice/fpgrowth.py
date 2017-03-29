from pyspark.mllib.fpm import FPGrowth
from pyspark import SparkContext
from datetime import datetime
import json
import re
import io

sc=SparkContext()


data = sc.textFile('/Users/francescobenetello/Documents/Dataset/sample.txt')

transactions = data.map(lambda line: line.strip().split(' '))
unique= transactions.map(lambda x: list(set(x)))

START_TIME = datetime.now()
model = FPGrowth.train(unique, minSupport=0.01, numPartitions=2)
END_TIME = datetime.now()

result = model.freqItemsets().collect()

for fi in result:
    print(fi)

TIME = format(END_TIME-START_TIME)
print("Tempo di esecuzione: " + str(TIME) + " sec")