from pyspark import SparkContext, SparkConf
import pyspark
import time
import json


# Creo un oggetto SparkContext
sc = SparkContext()

### Definisco PATH, SUPPORTO, GRANDEZZA SAMPLE
#filePath = '/stud/s3/fbenetel/WebIntelligence/Dataset/dataset_cleaned.txt'
filePath = '/Users/francescobenetello/Documents/Dataset/sample.txt'
# Lunghezza dataset 12230456
#lunghezzza ideal 12230448
SUPPORTO = 0.5
PARTIZIONI = 16
##############################################


# Apro il file

'''
def f(linea):
	return json.loads(linea)["text"]

set_of_tweets = openJSON.map(lambda linea: json.loads(linea)["text"])
'''
set_of_tweets = sc.textFile(filePath, PARTIZIONI)
### Calcolo la lunghezza dell'intero dataset
lineLengths = set_of_tweets.map(lambda s: len(s))
length = lineLengths.collect()
len = 0
for x in length:
	len += 1
print ("Lunghezza dataset " + str(len) + "\n") 
##############################################

def supporto_calcolato(supporto,dataset_len):
	return (supporto*(dataset_len//PARTIZIONI))/100


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


### Calcolo supporto minimo e lunghezza del sample
minsup = supporto_calcolato(SUPPORTO, len)
minsup = round(minsup)
print("Occorrenze >= " + str(minsup) + "\n") 
##################################################



START_TIME = time.time()

splitted = set_of_tweets.map(lambda line: line.split())
items = splitted.mapPartitions(word_occurence).filter(lambda (word, count): count >= minsup).glom().collect()


words = splitted.flatMap(word_pairs)
itemset = words.mapPartitions(word_occurence_pairs).filter(lambda (word, count): count >= minsup).glom().collect()

triples = splitted.flatMap(word_triple)
itemsets = triples.mapPartitions(word_occurence_pairs).filter(lambda (word, count): count >= minsup).glom().collect()
END_TIME = time.time()

num_partition = 1
print ("ITEMS")
for x in items:
	print ("PARTIZIONE NUMERO " + str(num_partition) + "\n")
	for y in x:
		print(y)
	print("FINE PARTIZIONE\n")
	num_partition += 1

num_partition = 1
print ("ITEMSET")
for x in itemset:
	print ("PARTIZIONE NUMERO " + str(num_partition) + "\n")
	for y in x:
		print(y)
	print("FINE PARTIZIONE\n")
	num_partition += 1

num_partition = 1
print ("ITEMSET")
for x in itemsets:
	print ("PARTIZIONE NUMERO " + str(num_partition) + "\n")
	for y in x:
		print(y)
	print("FINE PARTIZIONE\n")
	num_partition += 1

TIME =(END_TIME-START_TIME)
print("Tempo di esecuzione: " + str(TIME) + " secs")       			
