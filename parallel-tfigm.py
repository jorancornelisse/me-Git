from random import sample 
import time
from math import log10,sqrt

# second mapper arranges the documents for computhing N (total number of words in document)
def mapper2(result_reducer):
    total_output = []
    for line in result_reducer:
        # remove leading and trailing whitespace
        line = line.strip()
        # split the line into words
        word_filename, count=line.split("\t", 1)
        word_filename = word_filename.strip()
        try:
            class_word, filename = word_filename.split("*")
        except:
            pass
        try:
            clss, word = class_word.split("|")
        except:
            pass
        word = word.strip()
        output = word+ " " + count+ " | " + clss
        output = '%s \t %s' % (filename, output)
        total_output.append(output)

    return total_output


# computes N (total words in a document) by summing the n's per document
def reducer2(result_mapper):
    current_word = prev_filename = word = None
    current_count = N = 0
    data, l1, total_output = {}, [], []

    for line in result_mapper:
        line = line.strip()
        l1.append(line)
        filename, wordcount = line.split("\t", 1)
        wordcount_clss = wordcount.strip()
        wordcount, clss = wordcount.split("|", 1)
        word, count = wordcount.split()
        word, clss, count = word.strip(), clss.strip(), count.strip()
        clss, count = int(clss), int(count)
        if prev_filename == filename:
            N += count
        else:
            if prev_filename != None:
                data[prev_filename] = N
            N = 0
            prev_filename = filename
    data[prev_filename]=N

    for line in l1:
        filename,wordcount = line.split("\t", 1)
        wordcount_clss = wordcount.strip()
        wordcount, clss = wordcount.split("|", 1)
        word, count = wordcount.split()
        for row in data:
            if filename == k:
                wf = word + " " + filename
                nN = count + " " + str(df[k]) + "|" + clss
                output = '%s \t %s' % (wf, nN)
                total_output.append(output)
    return total_output


# Third mapper arranges words again, so class specific document frequencies can be computed
def mapper3(result_reducer):
    total_output = []
    for line in result_reducer:
        line = line.strip()
        wordfile, nN_clss = line.split("\t", 1)
        word, file_ = wordfile.split(" ",1)
        nN_clss = nN_clss.strip()
        nN, clss = nN_clss.split("|")
        output = file_ + " * " + nN + " " + "1" + " | " + clss
        output = "%s \t %s" % (word, output)
        total_output.append(output)
    return sorted(total_output)


# Calculate class-specific document frequency
def reducer3(result_mapper):
    lst1, lst2, total_output = [], [], []
    count = 1 
    for line in result_mapper:
        line = line.strip()
        word, clss = line.split("|")
        word, clss = word.strip(), clss.strip()
        if clss == "1":
            output = word + " | " + clss
            lst1.append(output)
        else:
            output = word + " | " + clss
            lst2.append(output)
    lst_total = [lst1 + lst2][0]

    prev_word = word = None
    for line in lst_total:
        line = line.strip()
        word, f_nNc_clss = line.split("\t", 1)
        word, f_nNc_clss = word.strip(), f_nNc_clss.strip()
        f, nNc_clss = f_nNc_clss.split("*", 1)
        f = f.strip()
        nNc_clss = nNc_clss.strip()
        nNc, clss= nNc_clss.split("|", 1)
        nNc, clss = nNc.strip(), clss.strip()
        n, Nc = nNc.split(" ", 1)
        N, c= Nc.split(" ", 1)
        if prev_word == w:
            count += int(c)
        else:
            if prev_word != None:
                output=w + " " + f + " \t " + n + " " + N + " " + str(count) + " | " + clss
                total_output.append(output)
            count = 1
            prev_word = w
    return total_output


# Sorting the words for adding the class-specific document frequencies together
def mapper4(result_reducer):
    counts_clss1, counts_clss2 = [], []
    wfs_clss1, wfs_clss2 = [], []
    total_output = []
    for line in result_reducer:
        line = line.strip()
        wf_nNc, clss = line.split("|", 1)
        wf, nNc = wf_nNc.split("\t", 1)
        nNc = nNc.strip()
        n, N, fkr = nNc.split()
        clss = clss.strip()
        wf = wf.strip()
        w, f = wf.split(" ",1)
        output = w + " " + clss + " \t " + n + " " + N + " \\t " + f + " \\\t " + fkr
        total_output.append(output)    
    return sorted(final_output)


# Adding the class-specific document frequencies together
def reducer4(result_mapper):
    total_output_1, total_output_2 = [], []
    prev_word = None
    prev_fkr = "1"
    for line in result_mapper:
        line = line.strip()
        w_class, nN_doc_fkr = line.split("\t", 1)
        nN_doc, fkr = nN_doc_fkr.split("\\\t", 1)
        fkr, w_class = fkr.strip(), w_class.strip()
        w, clss = w_class.split(" ", 1)
        w = w.strip()
        if prev_word == w:
            total = fkr + " " + prev_fkr
        elif (w == prev_word and len(str(prev_fkr)) == 1):
            total = fkr + " " + prev_fkr
        else:
            total = fkr
        prev_fkr = fkr
        prev_word = w
        output = w_class + " \t " + nN_doc + " \\\t " + total
        total_output_1.append(output)

    prev_word = None
    prev_fkr = '1'
    for line in reversed(total_output_1):
        line = line.strip()
        w_class, nN_doc_fkr = line.split("\t", 1)
        nN_doc, fkr = nN_doc_fkr.split(" \\\t ", 1)
        fkr = fkr.strip()
        w_class = w_class.strip()
        w, clss = w_class.split(" ", 1)
        w = w.strip()
        if (prev_word == w and len(str(prev_fkr)) > 1):
            total = prev_fkr
        else:
            total = fkr
        prev_fkr = fkr
        prev_word = w
        output = w_class + " \t " + nN_doc + " \\\t "+ total
        total_output_2.append(output)
    return total_output_2


# Calculating the TF-IGM values per term
def mapper5(result_reducer):
    lmbd = 1.5
    final_output = []
    for line in result_reducer:    
        line = line.strip()
        rest, fkrm = line.split("\\\t", 1)
        fkrm = fkrm.strip()
        w_clss_nN,doc =  rest.split("\\t", 1)
        w_clss_nN = w_clss_nN.strip()
        w_class, nN = w_clss_nN.split("\t", 1)
        nN = nN.strip()
        n, N = nN.split(" ", 1)
        n = int(n)
        N = int(N)
        fkrm = fkrm.strip()
        if len(fkrm) > 2:
            if " " in fkrm:
                fkr1, fkr2 = fkrm.split(" ", 1)
                fkr1, fkr2 = int(fkr1), int(fkr2)
                if N == 0:
                    N = 1
                igm = n/N * (1 + lmbd * (max([fkr1, fkr2]) / sum([fkr1 + fkr2])))
            else:
                igm = 0
        else:
            if N == 0:
                N = 1
            igm = (n/N)*(1+lmbd*1)
        output = w_class+ " " + doc + " \t "+ str(igm)
        final_output.append(output)
    return final_output