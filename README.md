# Running Instruction

### Dataset Building
Run the following scripts consecutively:
```
init.py
tag_tweets_py2.py
index.py
split.py
combine_data.sh
```

### Compile Spark Project
```
mvn clean package
```

### GD

Train GD
```
spark-submit --driver-memory 2g --class ca.uwaterloo.cs451.project.TrainSentimentClassifierGD \
             target/project-1.0.jar --input /shared/au/small_train_shuf.txt \
             --model small_train_shuf_gd --epoch 3 --regularization 0.0001 --lr 0.002
```
Test GD
```
spark-submit --driver-memory 2g --class ca.uwaterloo.cs451.project.ApplySentimentClassifier \
             target/project-1.0.jar --input /shared/au/small_test_shuf.txt \
             --model small_train_shuf_gd --output small_test_output_gd 

sh ./eval_hdfs.sh small_test_output_gd
 ```
 
### SGD 

Train SGD
```
spark-submit --driver-memory 2g --class ca.uwaterloo.cs451.project.TrainSentimentClassifierSGD \
             target/project-1.0.jar --input /shared/au/small_train_shuf.txt \
             --model small_train_shuf_sgd --epoch 3 --regularization 0.0001 --lr 0.002
```

Test SGD
```
spark-submit --driver-memory 2g --class ca.uwaterloo.cs451.project.ApplySentimentClassifier \
             target/project-1.0.jar --input /shared/au/small_test_shuf.txt \
             --model small_train_shuf_sgd --output small_test_output_sgd 

sh ./eval_hdfs.sh small_test_output_sgd
```
 
 ### MBSGD
 
 Train MBSGD
```
spark-submit --driver-memory 2g --class ca.uwaterloo.cs451.project.TrainSentimentClassifierMBSGD \
             target/project-1.0.jar --input /shared/au/small_train_shuf.txt \
             --model small_train_shuf_mbsgd --epoch 3 --regularization 0.0001 --lr 0.002 --fraction 0.1
```
Test MBSGD
```
spark-submit --driver-memory 2g --class ca.uwaterloo.cs451.project.ApplySentimentClassifier \
             target/project-1.0.jar --input /shared/au/small_test_shuf.txt \
             --model small_train_shuf_mbsgd --output small_test_output_mbsgd 

sh ./eval_hdfs.sh small_test_output_mbsgd
 ```

# Parameter Analysis on Epoch Number (epoch) 
`delta=0.002, lambda=0`

|     epoch    | SGD        | MBSGD (fraction=0.1)        | GD      |
| ------------- |:-------------:|:-------------:|:-------------:|
|   1    | 0.7424 |  0.7162 | 0.7177 | 
|   2    | 0.7452 | 0.7179 | 0.7177 | 
|   3    |  0.7424 | 0.7187 | 0.7178 | 
|   4    |0.7442  | 0.7200 | 0.7179 | 


# Parameter Analysis on Batch Size (fraction) 
`delta=0.002, epoch=3, lambda=0`

|     fraction   | MBSGD         |
| ------------- |:-------------:|
|   1    | 0.7178 | 
|   0.1    | 0.7187 | 
|   0.01    | 0.7327 | 
|   0.001    | 0.7438 | 


# Parameter Analysis on Learning Rate (delta) 
`lambda=0, epoch=3`

|     delta    | SGD        | MBSGD (fraction=0.1)        | GD      |
| ------------- |:-------------:|:-------------:|:-------------:|
|   0.001    | | 0.7173 |
|   0.002    | 0.7424 | 0.7187 | 0.7179 | 
|   0.005    | | 0.7231 |  |
|   0.01    | 0.7457 | 0.7283 |0.7192 | 
|   0.02    | | 0.7349 |
|   0.05    |0.7234  |  0.7444 | 0.7268 |
|   0.1    | 0.6757 |  0.7496 |0.7275
|   0.2    | 0.6670 |  0.7438 |0.7345
|   0.5    |  0.6937 | 0.6783  |0.6008


# Parameter Analysis on Regularization (lambda)
`epoch=1, delta=0.002`

|     lambda    | SGD     |
| ------------- |:-------------:|
|   0.0    | 0.7424 |
|   0.00001    | 0.7423 | 
|   0.0001    | 0.7438 | 
|   0.001    | 0.7438 |
|   0.01    | 0.7429 |
|   0.05    | 0.7426 |
|   0.5    | 0.7309 |



# Data Statistics

|         | train_all           | test_all  | train_small           | test_small  |
| ------------- |:-------------:|:-----:|:-----:|:-----:|
| File Size      | 25G | 6.1G | 361 M| 73 M|
| \# of Pos       | 33,870,264 | 8,467,134 | 500,206 | 99,918 |
| \# of Neg      | 33,869,640   |   8,467,758 | 499,794 | 100,082 |
| Avg \# of Characters     |  |  | | |
| Avg \# of Terms     |  |  | | |


# Best Performance

|      | test_all      | test_small  |
| ------------- |:-------------:|:-----:|
| GD | OOM |  0.7268 |
| SGD | OOM | 0.7457 |
| MBSGD | 0.7529 (fraction = 0.01) / OOM (fraction = 0.1)  |  0.7496 |


# Training and Testing on All Data through MBSGD

Train MBSGD
```             
spark-submit --deploy-mode client --num-executors 4 --executor-cores 4 --executor-memory 35G \
             --driver-memory 2g --class ca.uwaterloo.cs451.project.TrainSentimentClassifierMBSGD \
             target/project-1.0.jar --input /shared/au/train_all.txt \
             --model train_all_mbsgd --epoch 5 --regularization 0.0 --lr 0.02  --fraction 0.01      
```
Test MBSGD
```
spark-submit --driver-memory 2g --class ca.uwaterloo.cs451.project.ApplySentimentClassifier \
             target/project-1.0.jar --input /shared/au/test_all.txt \
             --model train_all_mbsgd --output test_all_output_mbsgd 

sh ./eval_hdfs.sh test_all_output_mbsgd
 ```

# Reference

* http://legacydirs.umiacs.umd.edu/~jimmylin/publications/Lin_Kolcz_SIGMOD2012.pdf
* https://courses.cs.washington.edu/courses/cse547/16sp/slides/logistic-SGD.pdf
* https://spark.apache.org/docs/latest/mllib-linear-methods.html
* https://github.com/apache/spark/tree/master/mllib/src/main/scala/org/apache/spark/mllib/optimization
* http://theory.stanford.edu/~tim/s16/l/l6.pdf

