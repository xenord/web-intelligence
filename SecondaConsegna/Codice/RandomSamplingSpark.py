__author__ = 'Francesco Benetello'

from pyspark import SparkContext, SparkConf
from datetime import datetime
import pyspark

# Creo un oggetto SparkContext
sc = SparkContext()

### Definisco PATH, SUPPORTO, GRANDEZZA SAMPLE, SEED
filePath = '/Users/francescobenetello/Documents/Dataset/sample.txt'
SUPPORTO = 0.5
GRANDEZZA_SAMPLE = 20
SEED = 1
##############################################


# Apro il file
set_of_tweets = sc.textFile(filePath)

### Calcolo la lunghezza dell'intero dataset
## Sample.json len = 12184
lineLengths = set_of_tweets.map(lambda s: len(s))
length = lineLengths.collect()
len = 0
for x in length:
	len += 1
##############################################

def supporto_calcolato(supporto,dataset_len):
	return (supporto*dataset_len)/100

### Calcolo supporto minimo e lunghezza del sample
subsetLength = (len*GRANDEZZA_SAMPLE)//100
minsup = supporto_calcolato(SUPPORTO, subsetLength)
print("Occorrenze >= " + str(minsup) + "\n")
print("Lunghezza sample: " + str(subsetLength) + "\n")
print("SEED: " + str(SEED) + "\n")
##################################################


# Prendo un campione random da analizzare
randomsampling = set_of_tweets.takeSample(False, subsetLength, SEED)
rdd1 = sc.parallelize(randomsampling)

#######
## Items
########
## Conto le occorrenze delle singole parole e 
## prendo quelle sopra al supporto prestabilito
##############################################

START_TIME = datetime.now()

words = rdd1.flatMap(lambda line: line.split(' '))
words_occurrences = words.map(lambda word: (word, 1)) \
	.reduceByKey(lambda v1, v2: v1+v2)
words_with_minsup = words_occurrences.filter(lambda (word, count): count >= minsup).collect()

END_TIME = datetime.now()
TIME1 = END_TIME-START_TIME

if words_with_minsup:
	print ("\n")
	print ("ITEMS => OCCORRENZE")
	print ("\n")
	for x in  words_with_minsup:
		print (x)


##########################################################
### Genero itemset
##########################################################
def word_pairs(rdd1):
    words = rdd1.split()
    return [a + " " + b for a,b in zip(words, words[1:])]

START_TIME = datetime.now()


pairs = rdd1.flatMap(word_pairs)

counter = pairs.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
rdd2 = counter.filter(lambda (x, count): count >= minsup).collect()
#rdd3 = rdd2.takeOrdered(subsetLength, key = lambda x: -x[1])

END_TIME = datetime.now()
TIME2 = END_TIME-START_TIME

if rdd2:
	print ("\n")
	print ("ITEMSET => OCCORRENZE")
	print ("\n")
	for x in  rdd2:
		print (x)

########
########

def word_triples(rdd1):
    words = rdd1.split()
    return [a + " " + b + " " + c for a,b,c in zip(words, words[1:], words[2:])]

START_TIME = datetime.now()

triples = rdd1.flatMap(word_triples)

counter = triples.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
rdd3 = counter.filter(lambda (x, count): count >= minsup).collect()

END_TIME = datetime.now()
TIME3 = END_TIME-START_TIME

if rdd3:
	print ("\n")
	print ("ITEMSET => OCCORRENZE")
	print ("\n")
	for x in  rdd3:
		print (x)

########
########

def word_quadruples(rdd1):
    words = rdd1.split()
    return [a + " " + b + " " + c + " " + d for a,b,c,d in zip(words, words[1:], words[2:], words[3:])]

START_TIME = datetime.now()

quadruples = rdd1.flatMap(word_triples)

counter = quadruples.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
rdd4 = counter.filter(lambda (x, count): count >= minsup).collect()

END_TIME = datetime.now()
TIME4 = END_TIME-START_TIME

if rdd4:
	print ("\n")
	print ("ITEMSET => OCCORRENZE")
	print ("\n")
	for x in  rdd4:
		print (x)

########


TIME = format(TIME1 + TIME2 + TIME3 + TIME4)

print ("\n")
print ("Tempo di esecuzione: " + str(TIME) + " secs")