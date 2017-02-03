import itertools
import time
import os
import codecs

def getItemSetValue(itemset,itemsetCollection):
    count = 1
    for everyItemSet in itemsetCollection:
        length = len(itemset)
        orgLen = len(itemset)
        index = 0
        length2 = 0
        while length > 0:
            if everyItemSet.count(itemset[index]) >= 1:
                length2 = length2 + 1
            length = length -1
            index = index + 1
        if length2 == orgLen:
            count = count + 1
    return count

def itemSetGenerator(previousItemset,width,refinedItemSet,minsupcount,newdataSet):
    comparingLength = width - 2
    #print(comparingLength)
    d = []
    temp = []
    for i in range(0,len(previousItemset)-1):
        firstSet = previousItemset[i]
        if(refinedItemSet[firstSet] >= minsupcount):
            for j in range(i+1, len(previousItemset)):
                secondSet = previousItemset[j]
                flag = 0
                if(refinedItemSet[secondSet] >= minsupcount):
                    for k in range(0,comparingLength):
                        if firstSet[k] == secondSet[k]:
                            flag = flag + 1
                    if flag == comparingLength:
                        for m in range(0,width-1):
                            temp.append(firstSet[m])
                        temp.append(secondSet[-1])
                        newSet = tuple(temp)
                        value = getItemSetValue(newSet,newdataSet)
                        if value >= minsupcount:
                            d.append(newSet)
                            refinedItemSet[newSet] = value
                        temp = []
    return d

def apriori(dataset, minsup):
    minsup = (minsup * len(dataset))/100
    originalItemset = dataset
    uniqueItemsValues = {}
    refinedItemSet = {}
    unrefinedItemset = {}
    itemset = []
    c = []
    for everyItemSet in originalItemset:

        everyItemSet = everyItemSet
        for everyItem in everyItemSet:
            everyItem = everyItem
            if everyItem not in uniqueItemsValues:
                uniqueItemsValues[everyItem] = 0
            if everyItem in uniqueItemsValues:
                uniqueItemsValues[everyItem] = uniqueItemsValues[everyItem] + 1

    for key, value in uniqueItemsValues.items():
        unrefinedItemset[key] = value
        if value >= minsup:
            refinedItemSet[key] = value
            itemset.append(key)

    itemset = sorted(itemset)

    for i in range(0,len(itemset)-1):
        for j in range(i+1,len(itemset)):
            #print(j)
            key = itemset[i],itemset[j]
            value = getItemSetValue(key,dataset)
            unrefinedItemset[key] = value
            if value >= minsup:
                refinedItemSet[key] = value
                c.append(key)

    width = 3
    while width > 0:
        newItemSet = itemSetGenerator(c,width,refinedItemSet,minsup,dataset)

        if(not newItemSet):
            break
        c = []
        c = newItemSet
        newItemSet = []
        width = width + 1

    print("FREQUENT ITEMSET =====>  OCCORRENZE")
    printed = 0
    count = 0
    while printed < len(refinedItemSet):
        for key,value in refinedItemSet.items():
            if type(key) == int and count < 1:
                print(str(key) + "  ========>  " + str(value))
                printed = printed+1

            elif type(key) != int:
                if len(key) == count:
                    print(str(key) + "  ========>  " + str(value))
                    printed = printed + 1
        count = count+1
    return refinedItemSet,uniqueItemsValues

if __name__ == '__main__':

    filePath = "10M_Tweets.txt"
    with open(filePath, encoding="utf8") as ins:
        array = []
        items = []
        array2 = []
        ln = 0
        for line in ins:
            ln=ln+1
            for item in line.split():
                items.append(item)
            array.append(line)
            array2.append(items)
            items = []

    print("Frequents Itemset con Random Sampling")

    # Selezionare qui sotto il SUPPORTO MINIMO
    MINUMUM_SUPPORT_THRESHOLD = 0.25
    SAMPLE_OF_SIZES = 50

    subsetLength = (len(array2)*SAMPLE_OF_SIZES)//100
    simpleRandomDataset = []
    dataset = {}
    for i in range(0,subsetLength):
        simpleRandomDataset.append(array2[i])
    START_TIME = time.time()
    dataset = apriori(simpleRandomDataset,MINUMUM_SUPPORT_THRESHOLD)
    END_TIME = time.time()
    print("Tempo di esecuzione: " + str(END_TIME-START_TIME))

