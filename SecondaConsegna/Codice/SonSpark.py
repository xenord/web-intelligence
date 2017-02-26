from pyspark import SparkContext, SparkConf
from datetime import datetime
import pyspark


# Creo un oggetto SparkContext
sc = SparkContext()

### Definisco PATH, SUPPORTO
PATH = "/Users/francescobenetello/Desktop/dataset_cleaned.txt"
SUPPORTO = 0.01
LUNGHEZZA_MAX_OUTPUT = 50
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

def supporto_calcolato(supporto,dataset_len):
	return (supporto*(dataset_len)/100)


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
minsup = supporto_calcolato(SUPPORTO, len)
minsup = round(minsup)
print("Occorrenze >= " + str(minsup) + "\n") 
##################################################


splitted = rdd.map(lambda line: line.split())
print ('Numero partizioni usate: ' + str(splitted.getNumPartitions()) + '\n')

START_TIME = datetime.now()

# ITEM
items = splitted.mapPartitions(word_occurence).filter(lambda (word, count): count >= minsup)
items = items.reduceByKey(lambda v1, v2: v1+v2).takeOrdered(LUNGHEZZA_MAX_OUTPUT, key = lambda x: -x[1])


# ITEMSET
words = splitted.flatMap(word_pairs)
itemset = words.mapPartitions(word_occurence_pairs).filter(lambda (word, count): count >= minsup)
itemset = itemset.reduceByKey(lambda v1, v2: v1+v2).takeOrdered(LUNGHEZZA_MAX_OUTPUT, key = lambda x: -x[1])

triples = splitted.flatMap(word_triple)
itemsets = triples.mapPartitions(word_occurence_pairs).filter(lambda (word, count): count >= minsup)
itemsets = itemsets.reduceByKey(lambda v1, v2: v1+v2).takeOrdered(LUNGHEZZA_MAX_OUTPUT, key = lambda x: -x[1])

quadruples = splitted.flatMap(word_quadruples)
itemsetss = quadruples.mapPartitions(word_occurence_pairs).filter(lambda (word, count): count >= minsup)
itemsetss = itemsetss.reduceByKey(lambda v1, v2: v1+v2).takeOrdered(LUNGHEZZA_MAX_OUTPUT, key = lambda x: -x[1])

END_TIME = datetime.now()

if items:
	for word, frequencies in items:
		print (word, frequencies)

if not items:
	print ("Non ci sono items da stampare!\n")

if itemset:
	for word, frequencies in itemset:
		print (word, frequencies)

if not itemset:
	print ("Non ci sono coppie da stampare!\n")

if itemsets:
	for word, frequencies in itemsets:
		print (word, frequencies)

if not itemsets:
	print ("Non ci sono triple da stampare!\n")

if itemsetss:
	for word, frequencies in itemsetss:
		print (word, frequencies)

if not itemsetss:
	print ("Non ci sono quadruple da stampare!\n")


TIME = format(END_TIME-START_TIME)
print("Tempo di esecuzione: " + str(TIME) + " secs\n")