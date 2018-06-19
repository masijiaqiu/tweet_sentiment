#!/bin/bash

files=${1}
#hadoop fs -cat ${files}/part-* | tr ',' ' ' | tr -d '()' | sed -e 's_pos_1_g' -e 's_neg_2_g' | sort -k3,3gr -t ' ' | ./compute_spam_metrics
hadoop fs -cat ${files}/part-* | tr ',' ' ' | tr -d '()' | sed -e 's_pos_1_g' -e 's_neg_2_g' | sort -k3,3gr -t ' ' > local_output.txt
python test.py
