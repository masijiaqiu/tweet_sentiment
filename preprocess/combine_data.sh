cat splitted/2013*train.txt > train_2013.txt
cat splitted/2014*train.txt > train_2014.txt
cat splitted/2015*train.txt > train_2015.txt
cat splitted/2016*train.txt > train_2016.txt
cat splitted/2017*train.txt > train_2017.txt
cat splitted/2018*train.txt > train_2018.txt

cat splitted/2013*test.txt > test_2013.txt
cat splitted/2014*test.txt > test_2014.txt
cat splitted/2015*test.txt > test_2015.txt
cat splitted/2016*test.txt > test_2016.txt
cat splitted/2017*test.txt > test_2017.txt
cat splitted/2018*test.txt > test_2018.txt

cat train_* > train_all.txt
cat test_* > test_all.txt
