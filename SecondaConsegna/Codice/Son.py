# -*- coding: utf-8 -*-

__author__ = "Francesco Benetello"
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


'''
frequent_items prende in input il set contenente i candidati generati da create_candidates e il dataset castato in set.
La scelta di usare un set deriva dal fatto che mi servirà la funzione issubset
'''

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

    create_candidates che prende in input il pattern e un dizionario inizialmente contente i frequent itemset della partizione
    e candidati di lunghezza pattern

    frequent_items che prende in input il set contenente i candidati generati da create_candidates e il dataset castato in set perchè dopo servirà
    la funzione issubset

    Ciclo while che conta a partire dalle coppie, vedi pattern = 2, le occorrenze all'interno della propria partizione(o nodo), 
    preleva quelle maggiori al min_sup_per_partitions, 
    se ci sono frequent itemset di lunghezza pattern li inserisce nella variabile se e inizia un nuovo ciclo con pattern + 1,
    altrimenti se non ci sono frequent itemset che superano il min_sup_per_partitions il ciclo termina e restituisce solo quelli frequenti

    I candidati vengono generati in base ai frequent itemset con pattern-1
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
            # Non posso passare itemset come rdd, ma per forza come collection
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

    # WARNING
    # Questa collection itemset, purtroppo sono costretto a ritornarla come collect altrimenti nella funzione foo dove
    # conto per la seconda volta le parole frequenti basandomi sui candidati calcolati in alto, mi da errore che non posso confrontare
    # direttamente due rdd
    itemset = cleaned.map(lambda x: x[0]).collect()

    # WARNING (vedi funzione foo2)
    # Confronto i frequent itemset candidati con il dataset (per la seconda volta) e conto quante volte compare un candidato nel dataset
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