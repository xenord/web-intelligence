from pyspark.mllib.fpm import FPGrowth
from pyspark import SparkContext

sc=SparkContext()

data = sc.textFile("10M_Tweets.txt")

transactions = data.map(lambda line: line.split(' '))
unique= transactions.map(lambda x: list(set(x))).cache()
model = FPGrowth.train(unique, minSupport=0.01, numPartitions=5)

result = model.freqItemsets().collect()

for fi in result:
    print(fi)

