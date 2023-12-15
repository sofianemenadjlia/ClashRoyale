rm -rf target/

mvn package

hdfs dfs -rm -r /user/smenadjlia/data-test/res*

rm data/part-r-00000

# yarn jar target/ClashRoyale-0.0.1.jar /user/smenadjlia/data-test/test.nljson /user/smenadjlia/data-test/seq

yarn jar target/ClashRoyale-0.0.1.jar 
# for i in {1..11}; do
#     yarn jar target/ClashRoyale-0.0.1.jar /user/auber/data_ple/clashroyale/gdc_battles.nljson /user/smenadjlia/data-test/res-month-$i 10 m-$i
# done

# for i in {1..52}; do
#     yarn jar target/ClashRoyale-0.0.1.jar /user/auber/data_ple/clashroyale/gdc_battles.nljson /user/smenadjlia/data-test/res-week-$i 15 s-$i
# done

# hdfs dfs -get /user/smenadjlia/data-test/res* data/results/ 

hdfs dfs -get /user/smenadjlia/data-test/res-all/part-r-00000 data/

# hdfs dfs -get /user/auber/data_ple/worldcitiespop.txt data/

# hdfs dfs -text /user/smenadjlia/data-test/seq/part-r-00000