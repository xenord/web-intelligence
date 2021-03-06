__author__ = 'Francesco Benetello'

'''
Questa classe si occupa principalmente, data una lista
di contare le occorrenze delle parole al suo interno
e di stampare le prime n parole ordinate per occorrenze in
senso descrescente.
'''
wordcount = {}
news = []

class CountWord:

    @staticmethod
    def word_occurence(news, wordcount):
        for word in news:
            if word not in wordcount:
                wordcount[word] = 1
            else:
                wordcount[word] += 1
        return wordcount

    @staticmethod
    def print_first_n_words(wordcount, how_many_times):
        i = 0
        for values, keys in sorted(wordcount.items(), reverse=True, key=lambda t: t[1]):
            if i < how_many_times:
                print (values, keys)
                i += 1
