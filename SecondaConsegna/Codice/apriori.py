import time


def getItemSetValue(itemset,itemsetCollection):
    count = 0
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

# newItemSet = itemSetGenerator(c,width,refinedItemSet,minsup,dataset)
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


def apriori(dataset,minisup):
    minsup = (minisup * len(dataset))
    #minconf = (minconf * len(dataset))/100
    originalItemset = dataset
    uniqueItemsValues = {}
    refinedItemSet = {}
    unrefinedItemset = {}
    itemset = []
    c = []

    # Conto i singletons
    for everyItemSet in originalItemset:
        #everyItemSet = everyItemSet
        for everyItem in everyItemSet:
            #everyItem = int(everyItem)
            if everyItem not in uniqueItemsValues:
                uniqueItemsValues[everyItem] = 0
            if everyItem in uniqueItemsValues:
                uniqueItemsValues[everyItem] = uniqueItemsValues[everyItem] + 1

    # Prelevo i singletons >= minsup e li inserisco in una lista
    for key, value in uniqueItemsValues.items():
        unrefinedItemset[key] = value
        if value >= minsup:
            refinedItemSet[key] = value
            itemset.append(key)

    # Ordino la lista
    itemset = sorted(itemset)

    # Creo coppie
    for i in range(0,len(itemset)-1):
        for j in range(i+1,len(itemset)):
            key = itemset[i],itemset[j]
            value = getItemSetValue(key,dataset)
            unrefinedItemset[key] = value
            if value >= minsup:
                refinedItemSet[key] = value
                # Creo la lista di sole chiavi
                c.append(key)

    # Creo dalle triple in su
    width = 3
    while width > 0:
        newItemSet = itemSetGenerator(c,width,refinedItemSet,minsup,dataset)

        if(not newItemSet):
            break
        c = []
        c = newItemSet
        newItemSet = []
        width = width + 1

    #print("FREQUENT ITEMSET =====>  COUNT")
    printed = 0
    count = 0
    while printed < len(refinedItemSet):
        for key,value in refinedItemSet.items():
            if type(key) == int and count < 1:
                #print(str(key) + "  ========>  " + str(value))
                printed = printed+1

            elif type(key) != int:
                if len(key) == count:
                    #print(str(key) + "  ========>  " + str(value))
                    printed = printed + 1
        count = count+1
    return refinedItemSet,uniqueItemsValues

if __name__ == '__main__':
    #print("###############  Loading the Transactions    ######################")

    ## Provide the Link to the data file here
    filePath = "/Users/francescobenetello/Documents/Dataset/simplest_sample.txt"
    with open(filePath, encoding="utf-8") as ins:
        #array = []
        items = []
        array2 = []
        ln = 0
        for line in ins:
            ln=ln+1
            for item in line.split():
                items.append(item)
            #array.append(line)
            array2.append(items)
            items = []
    #print("###############   "+ str(len(array2)) + "   Transactions Loaded Completely ################")
    #print("######## FREQUENT ITEMSET GENERATION USING THE APRIORI ALGORITHM #####################")

    '''
    ##### CHANGE THE VALUE OF MINIMUM SUPPORT AND CONFIDENCE HERE FOR APRIORI
    MINUMUM_SUPPORT_THRESHOLD = 20
    MINIMUM_CONFIDENCE_THRESHOLD = 5
    START_TIME = time.time()
    aprioriItemSet = apriori(array2,MINUMUM_SUPPORT_THRESHOLD,MINIMUM_CONFIDENCE_THRESHOLD)
    END_TIME = time.time()
    print("APRIORI EXECUTION TIME "+str(END_TIME-START_TIME))
    print("######################################## END OF APRIORI ALGORITHM ##########################################")

    print("############## ASSOCIATION RULE GENERATION FOR THE FREQUENT ITEMSET GENERATED ##############################")
    print(len(aprioriItemSet[0]))
    associationRules(aprioriItemSet[0],MINUMUM_SUPPORT_THRESHOLD,MINIMUM_CONFIDENCE_THRESHOLD,array2,aprioriItemSet[1])
    print("#############################")
    print("#############################")
    print("#############################")
    print("######## FREQUENT ITEMSET GENERATION USING THE SIMPLE RANDOM ALGORITHM #####################")
    '''

    ############# CHANGE THE NUMBER OF PARTITION HERE FOR SON ALGORITHM
    PARTITION = 2
    MINUMUM_SUPPORT_THRESHOLD = 0
    #MINIMUM_CONFIDENCE_THRESHOLD = 5
    print("THE NUMBER OF PARTITION USED IS "+ str(PARTITION))
    LIST_SON = []
    k = 0
    l = 0
    SON_DATA = {}
    DATA = {}
    TOTAL_TIME = []
    PARTITION_LENGTH = len(array2) / PARTITION
    for i in range(0,PARTITION):
        k = l
        l = l + PARTITION_LENGTH
        if i+1 == PARTITION:
            DATA[i] = array2[int(k):int(l+1)]
        else:
            DATA[i] = array2[int(k):int(l)]
    for key,value in DATA.items():
        a = ''
        START_TIME = time.time()
        a = apriori(value,MINUMUM_SUPPORT_THRESHOLD)
        END_TIME = time.time()
        TOTAL_TIME.append(END_TIME-START_TIME)
        for key1,val1 in a[0].items():
            if key1 in SON_DATA:
                SON_DATA[key1] = SON_DATA[key1] + val1
            else:
                SON_DATA[key1] = val1
    print("SON EXECUTION TIME " + str(sum(TOTAL_TIME)))
    '''
    print("## AFTER COMBINING ALL THE PARTITION THE SUM OF THE SIMILAR ITEMSET FROM DIFFERENT PARTITION IS #########")
    print(SON_DATA)
    print("###################################### END OF SON ALGORITHM ################################################")
    print("############## ASSOCIATION RULE GENERATION FOR THE FREQUENT ITEMSET GENERATED ##############################")
    '''
    #associationRules(SON_DATA,MINUMUM_SUPPORT_THRESHOLD,MINIMUM_CONFIDENCE_THRESHOLD,array2,aprioriItemSet[1])
