__author__ = "Francesco Benetello"

import time
import os
import re
import codecs
import io
import sys
from CountWord import CountWord

wordcount = {}

# Apriamo il file contenenti le stopwords inglesi
with io.open('stopwords_en.txt', encoding='utf-8') as f:
    stopwords = f.read().split('\n')

def clean_text(document):
        document_lower = document.lower()
        document_cleaned = re.sub('[!"#$%&\'()*+,-./:;<=>?@\[\\\\\]^_`{|}~]', ' ', document_lower)
        return document_cleaned.split()

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


cw = CountWord()
print ("Seleziona quale esercizio eseguire:")
print ("                                   ")
print ("                                   ")
print ("1 - trovare le 500 parole più frequenti (senza stopwords)   ---->   Press 1")
print ("                                   ")
print ("2 - Trovare le 500 parole più frequenti (stopwords comprese)     ---->  Press 2")
print ("                                   ")
print ("                                   ")
print ("3 - Rimuove i file TXT contenenti l'output stopwords_included/stopwords_removed   ---->  Press 3")
print ("                                   ")
n = int(input("Inserisci il numero delle esercizio da eseguire: "))


if (n == 1):
    def remove_stopwords(textcleaned):
    # filtro le parole (escludendo le stopwords inglesi)
        for word in textcleaned:
            if word not in stopwords:
                notizia_senza_stopwords.append(word)
        return notizia_senza_stopwords

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
                        notizia_senza_stopwords.append(word)
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
    cw.print_first_n_words(result_one, 5)
    sys.exit()

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

    cw.print_first_n_words(result_two, 5)
    sys.exit()

elif (n == 3):
    remove_file()
    sys.exit()

else:
    print ('Hai selezionato l esercizio sbagliato!')
    sys.exit()
