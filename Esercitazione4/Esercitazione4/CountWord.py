wordcount = {}
l = []

class CountWord:

    # prima di richiamare word_occurence
    # può essere necessario fare split()
    # del testo
    @staticmethod
    def word_occurence(text, wordcount):
        for word in text:
            if word not in wordcount:
                wordcount[word] = 1
            else:
                wordcount[word] += 1
        return wordcount

    @staticmethod
    def print_first_one_hundred_words(wordcount):
        i = 0
        for v, k in sorted(wordcount.items(), reverse=True, key=lambda t: t[1]):
            l.append(v)
            if i < 100:
                print(v)
                i+=1

