Completed

Tested on Altiscale

Tested on Linux: ubuntu1604-006

# Q1
group_x:

1-ROCA%: 17.25

group_y:

1-ROCA%: 12.82

group_britney:

1-ROCA%: 14.95
# Q2
average method:

1-ROCA%: 11.60
# Q3
vote method:

1-ROCA%: 14.60
# Q4
spam.train.all.txt:

1-ROCA%: 18.65
# Q5
1. 1-ROCA%: 17.40
2. 1-ROCA%: 16.34
3. 1-ROCA%: 15.76
4. 1-ROCA%: 21.88
5. 1-ROCA%: 15.47
6. 1-ROCA%: 18.21
7. 1-ROCA%: 16.74
8. 1-ROCA%: 13.23
9. 1-ROCA%: 13.91
10. 1-ROCA%: 18.50

spark-submit --driver-memory 2g --class ca.uwaterloo.cs451.a6.TrainSpamClassifier  target/assignments-1.0.jar --input /shared/uwaterloo/cs451/data/spam.train.britney.txt --model cs451-lintool-a6-model-britney --shuffle

spark-submit --driver-memory 2g --class ca.uwaterloo.cs451.a6.ApplySpamClassifier target/assignments-1.0.jar --input /shared/uwaterloo/cs451/data/spam.test.qrels.txt --output cs451-lintool-a6-test-britney --model cs451-lintool-a6-model-britney

./spam_eval_hdfs.sh cs451-lintool-a6-test-britney
