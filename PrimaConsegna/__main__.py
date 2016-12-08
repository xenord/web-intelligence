__author__ = "Francesco Benetello"

import time
import os
import re
import codecs
import io
import sys
import collections
import gensim
import math
from CountWord import CountWord
from gensim import utils
from nltk.stem import WordNetLemmatizer


wordcount = {}

# Apriamo il file contenenti le stopwords inglesi
with io.open('stopwords_en.txt', encoding='utf-8') as f:
    stopwords = f.read().split('\n')

# Riduciamo i caratteri in lowercase e rimuoviamo alcuni simboli
def clean_text(document):
        document_lower = document.lower()
        document_cleaned = re.sub('[!"#$%&\'()*+,-./:;<=>?@\[\\\\\]^_`{|}~]', ' ', document_lower)
        return document_cleaned.split()

# Rimuoviamo i file di testo contenente l'output
def remove_file():
    if (os.path.exists('stopwords_included.txt') & os.path.exists('stopwords_removed.txt')):
        os.remove("stopwords_included.txt")
        os.remove("stopwords_removed.txt")
        print ("Entrambi i Files Rimossi!")

    elif (os.path.exists('stopwords_included.txt')):
        os.remove('stopwords_included.txt')
        print("stopwords_included.txt rimosso!")

    elif os.path.exists('stopwords_removed.txt'):
        os.remove('stopwords_removed.txt')
        print("stopwords_removed.txt rimosso!")

    else:
        print ("Non ci sono file da rimuovere!")

def remove_stopwords(textcleaned):
    # filtro le parole (escludendo le stopwords inglesi)
        for word in textcleaned:
            if word not in stopwords:
                notizia_senza_stopwords.append(word)
        return notizia_senza_stopwords

# Creo un oggetto CountWord (si veda la classe CountWord)
cw = CountWord()
# Creo un oggetto WordNetLemmatizer (si veda nltk per ulteriori informazioni)
wordnet_lemmatizer = WordNetLemmatizer()

# Stampo un menù iterattivo con std input
print ("Seleziona quale esercizio eseguire:")
print ("                                   ")
print ("                                   ")
print ("1 - trovare le 500 parole più frequenti (senza stopwords, con lemmatize)  ---->   Press 1")
print ("                                   ")
print ("2 - Trovare le 500 parole più frequenti (stopwords comprese)     ---->  Press 2")
print ("                                   ")
print ("3 - Similarity (COSINE DISTANCE, TF-IDF, LSI)                       ---->  Press 3")
print ("                                   ")
print ("                                   ")
print ("                                   ")
print ("                                   ")
print ("4 - Rimuove i file TXT contenenti l'output stopwords_included/stopwords_removed   ---->  Press 4")
print ("                                   ")
print ("                                   ")
n = int(input("Inserisci il numero delle esercizio da eseguire: "))

# Caso in cui si scelga di avere 500 parole senza le stopwords
if (n == 1):

    notizia_senza_stopwords = []
    for root, dirs, files in os.walk('bbc'):
        for name in files:
            if ((name != '.DS_Store') & (name != 'README.TXT')):
                print (os.path.join(root, name))
                with codecs.open(os.path.join(root, name), encoding='utf-8', mode='r', errors='ignore')as f:
                    text = f.read()

                textcleaned = clean_text(text)
                for word in textcleaned:
                    if word not in stopwords:
                        word_lem = wordnet_lemmatizer.lemmatize(word)
                        notizia_senza_stopwords.append(word_lem)


        # Imposto una pausa di un sec tra l'apertura di una cartella e l'altra
        time.sleep(1)
    result_one = cw.word_occurence(notizia_senza_stopwords, wordcount)
    if os.path.exists('stopwords_removed.txt'):
        print ("File exists")

    else:
        i = 0
        with open("stopwords_removed.txt", 'w') as f:
            for values, keys in sorted(result_one.items(), reverse=True, key=lambda t: t[1]):
                if i < 500:
                    f.write(values + ' ' + str(keys) + '\n')
                    i += 1
    cw.print_first_n_words(result_one, 500)
    sys.exit()

# Caso in cui invece di scelga di vedere le stopwords all'interno dell'output
elif (n == 2):

    def ignore_stopwords(textcleaned):
    # filtro le parole (includendo le stopwords inglesi)
        for word in textcleaned:
            notizia_con_stopwords.append(word)
        return notizia_con_stopwords

    notizia_con_stopwords = []
    for root, dirs, files in os.walk('bbc'):
        for name in files:
            if ((name != '.DS_Store') & (name != 'README.TXT')):
                print (os.path.join(root, name))
                with codecs.open(os.path.join(root, name), encoding='utf-8', mode='r', errors='ignore')as f:
                    text = f.read()

                textcleaned = clean_text(text)
                for word in textcleaned:
                    notizia_con_stopwords.append(word)
        # Imposto una pausa di un sec tra l'apertura di una cartella e l'altra
        time.sleep(1)

    result_two = cw.word_occurence(notizia_con_stopwords, wordcount)
    if os.path.exists('stopwords_included.txt'):
        print ("File esistente! Non posso creare stopwords_included.txt")
        print ("Cancella stopwords_included.txt e riprova!")

    else:
        i = 0
        with open("stopwords_included.txt", 'w') as f:
            for values, keys in sorted(result_two.items(), reverse=True, key=lambda t: t[1]):
                if i < 500:
                    f.write(values + ' ' + str(keys) + '\n')
                    i += 1

    cw.print_first_n_words(result_two, 500)
    sys.exit()

# Punto riguardante la creazione di un context based e il calcolo
# della similitudine tra documenti
elif (n == 3):

    cartella_da_analizzare = 'bbc/tech'
    indice_documenti = []
    i = 0

    for root, dirs, files in os.walk(cartella_da_analizzare):
        for name in files:
            if ((name != '.DS_Store') & (name != 'README.TXT') & (i < 20)):
                print (os.path.join(root, name))
                with io.open(os.path.join(root, name), encoding='utf-8') as f:
                    text = f.read()

                textcleaned = clean_text(text)
                documenti = []
                for word in textcleaned:
                    if word not in stopwords:
                        documenti.append(word)
                indice_documenti.append(documenti)
                i += 1

    occurrences = collections.Counter()
    for text in indice_documenti:
        occurrences.update(text)

    # Rimuoviamo le parole con occorrenza 1
    for documenti in indice_documenti:
        for word in documenti:
            if occurrences[word] <= 1:
                documenti.remove(word)

    #for documenti in indice_documenti:
    #    print (documenti)


    lexicon = gensim.corpora.Dictionary(indice_documenti)
    corpus = [lexicon.doc2bow(text) for text in indice_documenti]


    '''
        FUNZIONE recommendations
        lexicon = dizionario
        corpus = vettore contenenti la coppia (parola, occorrenze)
        document = il numero del documento che si vuole calcolare la similarità

        esempio: recommendations(lexicon, corpus, corpus[19], n=5)
    '''

    def recommendations(lexicon, corpus, document, n=10):
        index = gensim.similarities.MatrixSimilarity(corpus, num_features=len(lexicon))
        scores = index[document]
        top = sorted(enumerate(scores), key=lambda t: t[1], reverse=True)
        return top[:n]

    print (recommendations(lexicon, corpus, corpus[19], n=5))

    '''
        Coseno similarity
    '''
    def length(vec):
	    return math.sqrt(sum([count*count for count in vec.values()]))

    def dot(vec1, vec2):
        # consideriamo solo le parole in comune tra vec1 e vec2
        wordsincommon = set(vec1.keys()) & set(vec2.keys())

        prods = []
        for word in wordsincommon:
            count1 = vec1[word]
            count2 = vec2[word]
            prods.append(count1*count2)

        return sum(prods)

    def similarity(vec1, vec2):
        return (dot(vec1, vec2) / (length(vec1)* length(vec2)))

    #similarities = [(i, similarity(lexicon[19], lexicon[i])) for i in range(len(lexicon))]


    '''
        Term Frequency * Inverse Document Frequency, Tf-Idf
    '''
    tfidf = gensim.models.TfidfModel(corpus, normalize=True)
    print ("Tf-Idf")
    print (recommendations(lexicon, tfidf[corpus], corpus[19], n=5))

    '''
        LSI (Latent Semantic Indexing)
    '''
    lsi = gensim.models.LsiModel(corpus, id2word=lexicon, num_topics=10)
    print ("LSI")
    print (recommendations(lexicon, lsi[corpus], lsi[corpus[19]], n=10))

    sys.exit()

# Quando viene scelto di fare pulizia dei file
elif (n == 4):
    remove_file()
    sys.exit()

# Messaggio di errore mostrato in caso di selezionamento errato
else:
    print ('Hai selezionato l esercizio sbagliato!')
    sys.exit()
