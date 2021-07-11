from random import sample 
import time
from math import log10, sqrt
from .constants import STOPWORDS

# Mapping words + filenames
def mapper1(data):
    total_output = []
    for line in data:
        filename = sample(lst_search, 1)[0]
        line = line.strip()
        unsorted = []
        words = line.split()
        for word in words:
            word = word.lower()
            if word not in STOPWORDS:
                word_file = word + " * " + filename
                output = "%s \t %s" % (word_file, 1)
                unsorted.append(output)
        sorted_output = sorted(unsorted)
        total_output.append(sorted_output)
    return output


# Computing the term occurences n
def reducer1(result_mapper):
    current_word, word = None, None
    current_count = 0
    total_output = []

    for result in result_mapper:
        for line in result:
            line = line.strip()
            word, count = line.split("\t", 1)

            try:
                count = int(count)
            except ValueError:
                continue
            
            # The if-switch works because the output is mapped by key (here: word),
            # this is how it also works in Hadoop
            if current_word == word:
                current_count += count
            else:
                if current_word:
                    output = "%s \t %s" % (current_word, current_count)
                    total_output.append(output)
                current_count = count
                current_word = word

    # Outputting the last word if needed
    if current_word == word:
        output = "%s \t %s" % (current_word, current_count)
        total_output.append(output)
    return final_output


def mapper2(result_reducer):
    total_output = []
    for line in result_reducer:
        # remove whitespace
        line = line.strip()
        # splitting into words
        wordfilename, count = line.split("\t",1)
        wordfilename = wordfilename.strip()
        try:
            word, filename=wordfilename.split("*")
        except:
            pass
        word_count = word + " " + count
        output = "%s \t %s" % (filename, word_count)
        total_output.append(output)
    return total_output


# Computing the document lengths
def reducer2(result_mapper):
    current_word = prev_filename = word = None
    current_count = N = 0
    data, l1, total_output = {}, [], []
    
    for line in result_mapper:
        line = line.strip()
        l1.append(line)
        filename, wordcount = line.split("\t", 1)
        wordcount = wordcount.strip()
        word, count = wordcount.split(" ", 1)
        count=int(count)
        if prev_filename == filename:
            N + =count
        else:
            if prev_filename != None:
                data[prev_filename] = N
            N=0
            prev_filename = filename
    data[prev_filename] = N

    for line in l1:
        filename, wordcount = h.split("\t", 1)
        wordcount = wordcount.strip()
        word, count = wordcount.split(" ", 1) 
        for file_ in data:
            if filename == file_:
                word_file = word +" "+ filename
                nN = count + " " + str(data[file_])
                output = "%s \t %s" % (word_file, nN)
                total_output.append(output)
    return total_output


# Sorting the words alphabetically
def mapper3(result_reducer):
    total_output = []
    for line in result_reducer:
        line = line.strip()
        word_file, nN = line.split("\t", 1)
        word, file_ = wf.split(" ", 1)
        file_nN = file_ + " + " + nN + " " +"1"
        output = '%s \t %s' % (word, file_nN)
        total_output.append(output)
    return sorted(total_output)


# Computing the document-frequency of the terms
def reducer3(result_mapper):
    prev_word = word = None
    count = 1 
    total_output = []
    for line in result_mapper:
        line = line.strip()
        word, file_nN = line.split("\t", 1)
        word = word.strip()
        file_nNc = file_nNc.strip()
        file_, nNc = file_nNc.split("*", 1)
        file_ = file_.strip()
        nNc = nNc.strip()
        n, Nc = nNc.split(" ", 1)
        N, c= Nc.split(" ", 1)
        if prev_word == word:
            count += int(c)
        else:
            if prev_word != None:
                output = word + " " + file_ + " \t " + n + " " + N + " " + str(count)
                total_output.append(output)
            count = 1
            prev_word = word
    return total_output

# Computing the TF-IDF weights
def mapper4(result_reducer):
    D = 1000
    total_output = []
    for line in result_reducer:
        # removing white space
        line = line.strip()
        # splitting the words
        word_file, nNm = line.split("\t", 1)
        word_file = word_file.strip()
        nNm = nNm.strip()
        n, N, m=nNm.split(" ", 2)
        # transform to numeric for computation
        n, N, m = float(n), float(N), float(m)
        if N == 0:
            tfidf = 0
        elif m == 0:
            tfidf = 0
        else:
            tfidf = (n/N) * log10(D/m)
        tfidf= (n/N) * log10(D/m)
        output = "%s \t %s" % (word_file,tfidf)
        total_output.append(output)
    return total_output