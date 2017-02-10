from pyspark import SparkContext, SparkConf
import pyspark
import time

# Creo un oggetto SparkContext
sc = SparkContext()

### Definisco PATH, SUPPORTO, GRANDEZZA SAMPLE
filePath = "/Users/francescobenetello/Documents/sample.txt"
SUPPORTO = 0.01
GRANDEZZA_SAMPLE = 20
PARTIZIONI = 3
##############################################


# Apro il file
set_of_tweets = sc.textFile(filePath).cache()


### Calcolo la lunghezza dell'intero dataset
## Sample.json len = 12184
lineLengths = set_of_tweets.map(lambda s: len(s))
length = lineLengths.collect()
len = 0
for x in length:
	len += 1
##############################################

def supporto_calcolato(supporto,dataset_len):
	return supporto*dataset_len

def f(hola):
	for x in hola:
		print(x)
	print ("## Divisore Partizione ##")

### Calcolo supporto minimo e lunghezza del sample
minsup = supporto_calcolato(SUPPORTO, len)
print(minsup)
##################################################



START_TIME = time.time()



splitted= set_of_tweets.map(lambda line: line.split())

ciao = splitted.collect()
hola = sc.parallelize(ciao, 3)
final = hola.glom()

i = 0
for x in final.collect():
	for y in x:
		z = sc.parallelize(y)
		words_occurrences = z.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1+v2)
		words_with_minsup = words_occurrences.filter(lambda (word, count): count >= 3).cache()
		ciao = words_with_minsup.collect()
		for w in ciao:
			print (w)
	break
	print("============================================================") 