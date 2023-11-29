rm -rf target/

mvn package

hdfs dfs -rm -r /user/smenadjlia/data-test/seq

rm data/part-r-00000

# yarn jar target/ClashRoyale-0.0.1.jar /user/smenadjlia/data-test/test.nljson /user/smenadjlia/data-test/seq

yarn jar target/ClashRoyale-0.0.1.jar /user/auber/data_ple/clashroyale/gdc_battles.nljson /user/smenadjlia/data-test/seq 15 a-0

hdfs dfs -get /user/smenadjlia/data-test/seq/part-r-00000 data/ 

# hdfs dfs -get /user/smenadjlia/data-test/test100.txt data/

# hdfs dfs -get /user/auber/data_ple/worldcitiespop.txt data/

# hdfs dfs -text /user/smenadjlia/data-test/seq/part-r-00000