import sys
import copy
import json
from string import atoi
from datetime import datetime
from pyspark import SparkContext, SparkConf
from collections import defaultdict
from itertools import imap, combinations

### Definisco PATH, SUPPORTO
PATH = '/Users/francescobenetello/Documents/Dataset/sample.txt'
SUPPORTO = 0.01
MAX_OUTPUT_LENGTH = 50
##############################################


def get_minimun_occurence(supporto,dataset_len):
    return (supporto*(dataset_len))


def create_candidates(tlist, pattern):
    list_of_pattern = []

    for i in tlist:
        for j in tlist:
            if len(i.union(j)) == pattern:
                join = i.union(j)
                list_of_pattern.append(join)

    return set(list_of_pattern)


# Checks occurrences of items in transactions and returns frequent items
# input:
#    items: a set of frequent items (candidates)
#     transactions: list of sets representing baskets
#  returns:
#      _itemSet: a set containing all frequent candidates (a subset of inputs)

def frequent_items(items, line_to_set):
    itemset = set()
    temp = {}

    for item in items:
        for transaction in line_to_set:
            if item.issubset(transaction):
                if item not in temp:
                    temp[item] = 1
                else:
                    temp[item] += 1

    for item, count in temp.items():
        if count >= min_sup_per_partitions:
            itemset.add(item)
    return itemset


def generate_frequent_itemset(data):
    line_to_set = []
    items = {}
    final = {}
    se = set()

    # Conto i singleton, e casto ogni riga del dataset in set
    for line in data:
        transaction = set(line)
        line_to_set.append(transaction)
        for element in transaction:
            if element not in items:
                items[element] = 1
            else:
                items[element] += 1

    '''
    Prelevo solo quelle che superano il supporto minimo per partizione
    Inserisco i candidati singoli in un frozenset, 
    perche unica collections che non da errore di unicode
    '''
    for item, count in items.iteritems():
        if count >= min_sup_per_partitions:
            se.add(frozenset([item]))

    '''

    '''
    steps=1
    pattern = 2
    final[pattern-1] = se
    while se != set([]) and pattern != steps:

        se = create_candidates(se,pattern)
        frequent_candidate = frequent_items(se,line_to_set)

        if  frequent_candidate != set([]): 
            final[pattern] = frequent_candidate

        se = frequent_candidate
        pattern += 1
        steps += 1

    return final.values()


def clean_set(list_of_list):
    wordcount = {}

    for tlist in list_of_list:
        for elem in tlist:
            if elem not in wordcount:
                wordcount[elem] = 1

    return wordcount.items()


def foo(list_of_list):
    wordcount = {}

    for tlist in list_of_list:
        for candidate in itemset:
            if tlist.issuperset(candidate):
                if candidate not in wordcount:
                    wordcount[candidate] = 1
                else:
                    wordcount[candidate] += 1

    return wordcount.items()






if __name__ == "__main__":

    # Creo un oggetto Spark
    sc  = SparkContext()

    # Apro RDD
    rdd = sc.textFile(PATH)

    # Ottengo il numero di partizioni in cui spark suddivide il proprio lavoro
    numPartitions = rdd.getNumPartitions()

    # Cerco la lunghezza del rdd
    lun = rdd.count()

    # Calcolo il numero minimo di occorrenze totale e per partizione
    minsup = get_minimun_occurence(SUPPORTO, lun)
    min_sup_per_partitions = minsup / numPartitions

    # Split riga per riga del dataset e rimuovo candidati dal basket
    splitted = rdd.map(lambda line: line.strip().split(' '))
    unique = splitted.map(lambda x: list(set(x))).map(lambda x: set(x))

    START_TIME = datetime.now()
    
    # Genero candidati, rimuovendo i duplicati e prendo solo le chiavi
    candidates = splitted.mapPartitions(generate_frequent_itemset)
    cleaned = candidates.mapPartitions(clean_set).reduceByKey(lambda a, b: a)
    itemset = cleaned.map(lambda x: x[0]).collect()

    # Confronto i candidati con il dataset e conto quante volte compare un candidato nel dataset
    counter = unique.mapPartitions(foo)

    # Merge di tutte le partizioni e filtro per minsup
    final = counter.reduceByKey(lambda x, y: x + y).filter(lambda (word, count): count >= minsup)
    END_TIME = datetime.now()
    TIME = format(END_TIME-START_TIME)

    final = final.map(lambda (itemset, count): ", ".join([str(x) for x in itemset])+"\t("+str(count)+")")

    number_of_results = final.count()

    print("\nFrequent Itemset \t Frequenza\n")
    for x in final.collect():
        print(x)

    print("Numero risultati: " + str(number_of_results)+ '\n')
    print("Tempo di esecuzione: " + str(TIME) + " sec")