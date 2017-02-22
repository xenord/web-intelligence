from pyspark import SparkContext, SparkConf
from datetime import datetime
import pyspark


# Creo un oggetto SparkContext
sc = SparkContext()

### Definisco PATH, SUPPORTO
PATH = '/Users/francescobenetello/Documents/Dataset/sample.txt'
SUPPORTO = 0.01
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
minsup = supporto_calcolato(SUPPORTO, len)
minsup = round(minsup)
print("Occorrenze >= " + str(minsup) + "\n") 
##################################################



START_TIME = datetime.now()

splitted = rdd.map(lambda line: line.split())
print ('Numero partizioni: ' + str(splitted.getNumPartitions()))
# ITEM
items = splitted.mapPartitions(word_occurence).filter(lambda (word, count): count >= minsup).glom().collect()

# ITEMSET
words = splitted.flatMap(word_pairs)
itemset = words.mapPartitions(word_occurence_pairs).filter(lambda (word, count): count >= minsup).glom().collect()

triples = splitted.flatMap(word_triple)
itemsets = triples.mapPartitions(word_occurence_pairs).filter(lambda (word, count): count >= minsup).glom().collect()

quadruples = splitted.flatMap(word_quadruples)
itemsetss = quadruples.mapPartitions(word_occurence_pairs).filter(lambda (word, count): count >= minsup).glom().collect()

END_TIME = datetime.now()

num_partition = 1
empty_flag = 1
for x in items:
	if x:
		print ("PARTIZIONE NUMERO " + str(num_partition) + "\n")
		for y in x:
			print(y)
		print("FINE PARTIZIONE\n")
		num_partition += 1
	else:
		empty_flag += 1
		num_partition += 1

if (empty_flag == num_partition):
	print ("Non ci sono ITEMS.")


num_partition = 1
empty_flag = 1
for x in itemset:
	if x:
		print ("PARTIZIONE NUMERO " + str(num_partition) + "\n")
		for y in x:
			print(y)
		print("FINE PARTIZIONE\n")
		num_partition += 1
	else:
		empty_flag += 1
		num_partition += 1

if (empty_flag == num_partition):
	print ("Non ci sono coppie di ITEMSET.")

num_partition = 1
empty_flag = 1
for x in itemsets:
	if x:
		print ("PARTIZIONE NUMERO " + str(num_partition) + "\n")
		for y in x:
			print(y)
		print("FINE PARTIZIONE\n")
		num_partition += 1
	else:
		empty_flag += 1
		num_partition += 1

if (empty_flag == num_partition):
	print ("Non ci sono triple di ITEMSET.")

num_partition = 1
empty_flag = 1
for x in itemsetss:
	if x:
		print ("PARTIZIONE NUMERO " + str(num_partition) + "\n")
		for y in x:
			print(y)
		print("FINE PARTIZIONE\n")
		num_partition += 1
	else:
		empty_flag += 1
		num_partition += 1

if (empty_flag == num_partition):
	print ("Non ci sono  quadruple di ITEMSET.")

TIME = format(END_TIME-START_TIME)
print("Tempo di esecuzione: " + str(TIME) + " secs")       			