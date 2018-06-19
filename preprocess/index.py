#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import json
import sys
import re
import os
import multiprocessing
import time 

def index(input_file):
    ind = 1
    print("input_file: {}".format(input_file))
    with open(input_file, 'r') as inf:
        date = input_file.split("/")[-1].split(".")[0]
        output_file = "indexed/{}.indexed".format(date) 
        with open(output_file, 'w', 100) as outf:
            num = 1
            for line in inf:
                record = json.loads(line)
                if u'text' in record:
                    text = []
                    unicode_text = record[u'text']
                    label = 'pos' if record[u'label'] == '1' else 'neg'
                    unicode_text = unicode_text.lower()
                    for i in range(len(unicode_text)-4):
                        ngram = unicode_text[i:i+4]
                        text.append(str(abs(hash(ngram)) % (10 ** 6)))
                    ID = date + "-" + str(num)
                    num += 1
                    outf.write("{} {} {}\n".format(ID, label, " ".join(text)))


def run(data_dir):
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir)]
    start = time.time()
    pool = multiprocessing.Pool(60)
    results = pool.map_async(index, files)
    pool.close()
    pool.join()
    print(time.time()-start)

if __name__ == '__main__':
    run("/home/w85yang/preprocess/tagged")

