from pyspark.mllib.fpm import FPGrowth
from pyspark import SparkContext
import json

sc=SparkContext()

data = sc.textFile("/Users/francescobenetello/Documents/Dataset/sample.txt")
#openJSON = sc.textFile('/Users/francescobenetello/Documents/Dataset/sample.json')
'''
def f(linea):
	return json.loads(linea)["text"]

data = openJSON.map(lambda linea: json.loads(linea)["text"])
'''
transactions = data.map(lambda line: line.split(' '))
unique= transactions.map(lambda x: list(set(x))).cache()
model = FPGrowth.train(unique, minSupport=0.01, numPartitions=2)

result = model.freqItemsets().collect()

for fi in result:
    print(fi)

