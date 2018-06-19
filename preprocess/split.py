#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import json
import sys
import re
import os
import multiprocessing
import time 

def split(input_file):
    date = input_file.split("/")[-1].split(".")[0]
    test_pos = open("splitted/{}_pos_test.txt".format(date), "w")
    test_neg = open("splitted/{}_neg_test.txt".format(date), "w")
    train_pos = open("splitted/{}_pos_train.txt".format(date), "w")
    train_neg = open("splitted/{}_neg_train.txt".format(date), "w")

    with open(input_file, 'r') as inf:
        pos = 0
        neg = 0
        for line in inf:
            label = line.split()[1]
            if label == "pos":
                pos += 1
            else:
                neg += 1

    num = min(pos, neg)
    print("input_file: {} min: {}, max: {}".format(input_file, num, max(pos, neg)))

    with open(input_file, 'r') as inf:
        i = 0
        pos = 0
        neg = 0
        for line in inf:
            label = line.split()[1]
            if label == "pos":
                pos += 1
                if pos < num:
                    i += 1
                    if i % 10 < 2:
                        test_pos.write(line)
                    else: 
                        train_pos.write(line)
            else:
                neg += 1
                if neg < num:
                    i += 1
                    if i % 10 < 2:
                        test_neg.write(line)
                    else:
                        train_neg.write(line)
            
    test_pos.close()
    test_neg.close()
    train_pos.close()
    train_neg.close()

def run(data_dir):
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir)]
    start = time.time()
    pool = multiprocessing.Pool(60)
    results = pool.map_async(split, files)
    pool.close()
    pool.join()
    print(time.time()-start)

if __name__ == '__main__':
    run("/home/w85yang/preprocess/indexed")
