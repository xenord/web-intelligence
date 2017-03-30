from pyspark import SparkContext, SparkConf
from collections import defaultdict
from itertools import imap, combinations
from datetime import datetime
from string import atoi
import pyspark
import itertools
import json
import io
import re
import sys
import copy
import math


### Definisco PATH, SUPPORTO
PATH = '/stud/s3/fbenetel/sample.txt'
SUPPORTO = 0.005
MAX_OUTPUT_LENGTH = 50
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

'''
getMatrix
Questa funzione prende un input un pyspark.RDD
nel nostro caso gli viene passato il dataset.
Per ogni tweet mantiene solo le parole frequente.

es. Lista di parole frequenti: A B D
    Dataset: A B C
             D F H
             A D F

    Risultato: A B
               D
               A D
'''
def getMatrix(list_of_list):
    matrix = []
    word = []

    for x in list_of_list:
        i = (len (x))
        for y in range(0, i):
            if x[y] in items_only:
                word.append(x[y])
        if word:
            matrix.append(word)
        word = []
             
    return matrix


def getFrequencies(list_of_list):
    wordcount = {}
    for x in list_of_list:
        y = tuple(x)
        if y not in wordcount:
            wordcount[y] = len(y)

    return wordcount.items()



'''
Combinazioni semplici = n!/k!(n-k)! dove n e' la lunghezza del tweet(composto da parole frequenti)
generati da questo frequent_items = remove_duplicates(items_as_array)
Mentre k e' il raggruppamento, cioe' se si cercano coppie sara' k=2, triple k=3 e cosi via
'''
def generate_frequent_itemset(list_of_list):
    wordcount = {}
    tlist = []

    for k,v in list_of_list:
        for pattern in range(1, v):
            z = list(combinations(k, pattern))
            for itemset in z:
                if itemset not in tlist:
                    tlist.append(itemset)

    for elem in tlist:
        t = tuple(elem)
        if t not in wordcount:
            wordcount[t] = 0

    return wordcount.items()



'''
Calcolo coefficiente binomiale

lunghezza_tweet: lunghezza del tweet formato solo da parole frequenti
lunghezza_itemset: lunghezza del itemset (coppie, triple, quadruple ecc)
'''
def combinazioni_per_itemset(lunghezza_tweet, lunghezza_itemset):
    coeff_binom = (math.factorial(lunghezza_tweet))/(math.factorial(lunghezza_itemset)*(math.factorial(lunghezza_tweet-lunghezza_itemset)))
    return coeff_binom




sc = SparkContext()

rdd = sc.textFile(PATH)

lun = rdd.count()
print('\n')
print ("Lunghezza dataset " + str(lun) + "\n") 

minsup = get_minimun_occurence(SUPPORTO, lun)
minsup = round(minsup)
print("Occorrenze >= " + str(minsup) + "\n") 

splitted = rdd.map(lambda line: [y for y in line.strip().split(' ')])
unique = splitted.map(lambda x: list(set(x)))
print ('Numero partizioni: ' + str(splitted.getNumPartitions()) + '\n')

items = unique.mapPartitions(word_occurence).reduceByKey(lambda a, b: a + b).filter(lambda (word, count): count >= minsup)



items_only = items.map(lambda x: x[0]).collect()


'''
Poco efficienti

Guarda per generare frequent itemset:
http://stackoverflow.com/questions/4059550/generate-all-possible-strings-from-a-list-of-token
'''
items_as_array = unique.mapPartitions(getMatrix)
items_as_array_cleaned = items_as_array.mapPartitions(getFrequencies).reduceByKey(lambda a, b: a)
final = items_as_array_cleaned.mapPartitions(generate_frequent_itemset).reduceByKey(lambda a, b: a)


casted = final.map(lambda x: x[0]).collect()
setted = unique.map(lambda x: set(x)).collect()

settiamo = []

for x in casted:
    z = set(x)
    if z not in settiamo:
        settiamo.append(z)


ciao = {}
for x in setted:
    for y in settiamo:
        if x.issuperset(y):
            t = tuple(y)
            if t not in ciao:
                ciao[t] = 1
            else:
                ciao[t] += 1

for k,v in ciao.items():
    if v >= minsup:
        print(k,v)

#l1 = ['ciao','mi','chiamo','francesco']
#l2 = ['ciao','francesco']
