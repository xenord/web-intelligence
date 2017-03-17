__author__ = 'Francesco Benetello'

from Utils import Utils
from pyspark import SparkContext, SparkConf
import pyspark
import time

# Definisco come COSTANTI
filePath = '/Users/francescobenetello/Documents/Dataset/sample.txt'
SUPPORTO = 0.5
GRANDEZZA_SAMPLE = 20
SEED = 10
###


sc = SparkContext()

rdd = sc.textFile(filePath)
u = Utils(rdd, SUPPORTO, GRANDEZZA_SAMPLE, False)

u.SubsetLength()

u.length_dataset()
print (u.getDatasetLength())

u.minsupport_for_random_sampling()
minsup = u.getMinimunSupport()
print (minsup)


randomsampling = rdd.takeSample(False, u.getSubsetLength(), seed=15)
rdd1 = sc.parallelize(randomsampling)


# Conto le occorrenze delle singole parole e 
# prendo quelle sopra al supporto prestabilito

words = rdd1.flatMap(lambda line: line.split(' '))
# Items
words_occurrences = words.map(lambda word: (word, 1)) \
	.reduceByKey(lambda v1, v2: v1+v2)
items = words_occurrences.filter(lambda (word, count): count >= minsup).glom().collect()

def word_pairs(rdd1):
	words = rdd1.split()
	return [a + " " + b for a,b in zip(words, words[1:])]

pairs = words.flatMap(word_pairs)
counter = pairs.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
itemsets_pairs = counter.filter(lambda (x, count): count >= minsup).collect()

for x in items:
	print (x)

print(type(itemsets_pairs))