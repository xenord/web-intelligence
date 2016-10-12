__author__ = "Francesco Benetello"

import urllib2
import time
from CountWord import CountWord

wordcount = {}

in_file = open("ANSA_UrlList.txt", "r")
text = in_file.read()
in_file.close()

"""
cw = CountWord()
result = cw.word_occurence(text, wordcount)
cw.print_first_one_hundred_words(result)
"""

try:
    for url in text.split():
        page = urllib2.urlopen(url)
        time.sleep(1)
        # for line in page:
        #   print line
except urllib2.HTTPError:
    print ('Pagina non disponibile!')
    print ('Errore 404!')
