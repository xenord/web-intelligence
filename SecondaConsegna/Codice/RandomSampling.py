__author__ = 'Francesco Benetello'

from pyspark import SparkContext, SparkConf
from datetime import datetime
import pyspark


### Definisco PATH, SUPPORTO, GRANDEZZA SAMPLE, SEED
filePath = '/Users/francescobenetello/Documents/Dataset/sample.txt'
SUPPORTO = 0.5
GRANDEZZA_SAMPLE = 20
SEED = 1
##############################################

def supporto_calcolato(supporto,dataset_len):
    return (supporto*dataset_len)/100

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
    
    # Creo un oggetto SparkContext
    sc = SparkContext()
    # Apro il file
    rdd = sc.textFile(filePath)

    ### Calcolo la lunghezza dell'intero dataset
    lun = rdd.count()

    ### Calcolo supporto minimo e lunghezza del sample
    subsetLength = (lun*GRANDEZZA_SAMPLE)//100
    minsup = supporto_calcolato(SUPPORTO, subsetLength)

    print("Occorrenze >= " + str(minsup) + "\n")
    print("Lunghezza sample: " + str(subsetLength) + "\n")
    print("SEED: " + str(SEED) + "\n")

    randomsampling = rdd.takeSample(False, subsetLength, SEED)
    sample = sc.parallelize(randomsampling)

    numPartitions = sample.getNumPartitions()
    print("Numero partizioni: " +str(numPartitions))
    min_sup_per_partitions = minsup / numPartitions

    splitted = sample.map(lambda line: line.strip().split(' '))
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
