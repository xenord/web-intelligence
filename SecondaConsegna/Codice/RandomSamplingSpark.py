from pyspark import SparkContext, SparkConf
import pyspark
import time

# Creo un oggetto SparkContext
sc = SparkContext()

### Definisco PATH, SUPPORTO, GRANDEZZA SAMPLE
filePath = '/Users/francescobenetello/Documents/Dataset/sample.txt'
SUPPORTO = 0.01
GRANDEZZA_SAMPLE = 20
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
	return supporto*dataset_len
### Calcolo supporto minimo e lunghezza del sample
subsetLength = (len*GRANDEZZA_SAMPLE)//100
minsup = supporto_calcolato(SUPPORTO, subsetLength)
print(minsup)
print(subsetLength)
##################################################


# Prendo un campione random da analizzare
START_TIME = time.time()
randomsampling = set_of_tweets.takeSample(False, subsetLength, seed=15)
rdd1 = sc.parallelize(randomsampling)


# Conto le occorrenze delle singole parole e 
# prendo quelle sopra al supporto prestabilito

words = rdd1.flatMap(lambda line: line.split(' '))
words_occurrences = words.map(lambda word: (word, 1)) \
	.reduceByKey(lambda v1, v2: v1+v2)
words_with_minsup = words_occurrences.filter(lambda (word, count): count >= minsup).cache()
rdd4 = words_with_minsup.collect()
print("-------------------------------------------")
print("| Items                        Occorrenze |")
print("-------------------------------------------")


for x in rdd4:
	print (x)

### Genero itemset
def word_pairs(rdd1):
    words = rdd1.split()
    return [a + " " + b for a,b in zip(words, words[1:])]

pairs = rdd1.flatMap(word_pairs)
print (type(rdd1))
print (type(pairs))
counter = pairs.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
rdd2 = counter.filter(lambda (x, count): count >= minsup)
rdd3 = rdd2.takeOrdered(subsetLength, key = lambda x: -x[1])
###

#fo = rdd3.collect()
#rdd3 = words_with_minsup.cartesian(rdd2)

#frequentoccurrences = rdd3.filter(lambda (word, count): count >= minsup)
END_TIME = time.time()
TIME =(END_TIME-START_TIME)
#fo = frequentoccurrences.collect()

print("-------------------------------------------")
print("| Itemset                      Occorrenze |")
print("-------------------------------------------")

for y in rdd3:
	print (y)
print("Tempo di esecuzione: " + str(TIME) + " secs")
