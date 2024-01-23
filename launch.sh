rm -rf target/
mvn clean
mvn package

# spark-submit --class topk.TopKDecks --master yarn --num-executors 4 --total-executor-cores 8 --executor-memory 512M  target/ClashRoyale-0.0.1.jar

spark-submit --class topk.TopKDecks --master yarn target/ClashRoyale-0.0.1.jar


# hdfs dfs -rm -r /user/smenadjlia/data-test/res-all

# rm data/part-r-00000

# yarn jar target/ClashRoyale-0.0.1.jar

# hdfs dfs -get /user/smenadjlia/data-test/res-all/part-r-00000 data/


















# yarn jar target/ClashRoyale-0.0.1.jar /user/smenadjlia/data-test/test.nljson /user/smenadjlia/data-test/seq

# yarn jar target/ClashRoyale-0.0.1.jar /user/auber/data_ple/clashroyale/gdc_battles.nljson /user/smenadjlia/data-test/res-all 10 a-0

# for i in {1..11}; do
#     yarn jar target/ClashRoyale-0.0.1.jar /user/auber/data_ple/clashroyale/gdc_battles.nljson /user/smenadjlia/data-test/res-month-$i 10 m-$i
# done

# for i in {1..52}; do
#     yarn jar target/ClashRoyale-0.0.1.jar /user/auber/data_ple/clashroyale/gdc_battles.nljson /user/smenadjlia/data-test/res-week-$i 15 s-$i
# done

# hdfs dfs -get /user/smenadjlia/data-test/res* data/results/ 

# hdfs dfs -get /user/smenadjlia/data-test/seq/part-r-00000 data/

# hdfs dfs -get /user/auber/data_ple/worldcitiespop.txt data/

# hdfs dfs -text /user/smenadjlia/data-test/seq/part-r-00000