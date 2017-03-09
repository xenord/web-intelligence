from pyspark import SparkContext, SparkConf
from datetime import datetime
import pyspark


# Creo un oggetto SparkContext
sc = SparkContext()

### Definisco PATH, SUPPORTO
PATH = "/stud/s3/fbenetel/WebIntelligence/Dataset/dataset_cleaned.txt"
SUPPORTO = 0.006
MAX_OUTPUT_LENGTH = 50
##############################################


rdd = sc.textFile(PATH)
### Calcolo la lunghezza dell'intero dataset
lineLengths = rdd.map(lambda s: len(s))
length = lineLengths.collect()
len = 0
for x in length:
	len += 1
print ("Lunghezza dataset " + str(len) + "\n") 
##############################################

def get_minimun_occurence(supporto,dataset_len):
	return (supporto*(dataset_len))


def word_occurence(list_of_list):
	wordcount = {}
	for tlist in list_of_list:
		for word in tlist:
			if word not in wordcount:
				wordcount[word] = 1
			else:
				wordcount[word] += 1

	return wordcount.items()

def word_occurence_pairs(tlist):
	wordcount = {}
	for word in tlist:
		if word not in wordcount:
			wordcount[word] = 1
		else:
			wordcount[word] += 1

	return wordcount.items()


def word_pairs(words):
	return [a + " " + b for a,b in zip(words, words[1:])]

def word_triple(words):
	return [a + " " + b + " " + c for a,b,c in zip(words, words[1:], words[2:])]

def word_quadruples(words):
    return [a + " " + b + " " + c + " " + d for a,b,c,d in zip(words, words[1:], words[2:], words[3:])]


### Calcolo supporto minimo e lunghezza del sample
minsup = get_minimun_occurence(SUPPORTO, len)
minsup = round(minsup)
print("Occorrenze >= " + str(minsup) + "\n") 
##################################################



START_TIME = datetime.now()

splitted = rdd.map(lambda line: line.split())
print ('Numero partizioni: ' + str(splitted.getNumPartitions()))
# ITEM
items = splitted.mapPartitions(word_occurence).reduceByKey(lambda a, b: a + b).filter(lambda (word, count): count >= minsup).take(MAX_OUTPUT_LENGTH)

# ITEMSET
words = splitted.flatMap(word_pairs)
itemset = words.mapPartitions(word_occurence_pairs).reduceByKey(lambda a, b: a + b).filter(lambda (word, count): count >= minsup).take(MAX_OUTPUT_LENGTH)

triples = splitted.flatMap(word_triple)
itemsets = triples.mapPartitions(word_occurence_pairs).reduceByKey(lambda a, b: a + b).filter(lambda (word, count): count >= minsup).take(MAX_OUTPUT_LENGTH)

quadruples = splitted.flatMap(word_quadruples)
itemsetss = quadruples.mapPartitions(word_occurence_pairs).reduceByKey(lambda a, b: a + b).filter(lambda (word, count): count >= minsup).take(MAX_OUTPUT_LENGTH)

END_TIME = datetime.now()

if items:
	for x in items:
		print(x)

if not items:
	print("Vuoto!")

if itemset:
	for x in itemset:
		print(x)

if not itemset:
	print("Non ci sono coppie di valori!")

if itemsets:
	for x in itemsets:
		print(x)

if not itemsets:
	print("Non ci sono triple di valori!")

if itemsetss:
	for x in itemsetss:
		print(x)

if not itemsetss:
	print("Non ci sono quadruple di valori!")


TIME = format(END_TIME-START_TIME)
print("Tempo di esecuzione: " + str(TIME) + " secs")       			