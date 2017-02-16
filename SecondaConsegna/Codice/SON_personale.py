from pyspark import SparkContext, SparkConf
import pyspark
import time
import json

# print(type())

# Creo un oggetto SparkContext
sc = SparkContext()

### Definisco PATH, SUPPORTO, GRANDEZZA SAMPLE
filePath = '/Users/francescobenetello/Documents/Dataset/sample.txt'
SUPPORTO = 0.001
PARTIZIONI = 3
##############################################


# Apro il file
'''
openJSON = sc.textFile(filePath, PARTIZIONI)

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
print (len)
##############################################

def supporto_calcolato(supporto,dataset_len):
	return supporto*(dataset_len)


def word_occurence(list_of_list):
	wordcount = {}
	for tlist in list_of_list:
		for word in tlist:
			if word not in wordcount:
				wordcount[word] = 1
			else:
				wordcount[word] += 1

	return wordcount.items()


def word_pairs(list_of_list):
	for tlist in list_of_list:
		for words in tlist:
			return [a + " " + b for a,b in zip(words, words[1:])]


### Calcolo supporto minimo e lunghezza del sample
minsup = supporto_calcolato(SUPPORTO, len)
print(minsup)
##################################################



START_TIME = time.time()

splitted = set_of_tweets.map(lambda line: line.split())
words_occurence = splitted.mapPartitions(word_occurence).cache()

# Ottengo gli items e itemset
items = words_occurence.filter(lambda (word, count): count >= minsup).glom().collect()
itemset = words_occurence.mapPartitions(word_pairs).filter(lambda (word, count): count >= minsup).glom().collect()

num_partition = 1
print ("Items")
for x in items:
	print ("PARTIZIONE NUMERO " + str(num_partition) + "\n")
	for y in x:
		print(y)
	print("FINE PARTIZIONE\n")
	num_partition += 1

num_partition = 1
print ("Itemset")
for x in itemset:
	print ("PARTIZIONE NUMERO " + str(num_partition) + "\n")
	for y in x:
		print(y)
	print("FINE PARTIZIONE\n")
	num_partition += 1

END_TIME = time.time()
TIME =(END_TIME-START_TIME)
print("Tempo di esecuzione: " + str(TIME) + " secs")       			