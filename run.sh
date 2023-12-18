rm -rf target/

mvn package

# hdfs dfs -rm -r /user/smenadjlia/data/deck-stats/*

rm data/part-r-00000

# yarn jar target/ClashRoyale-0.0.1.jar /user/smenadjlia/data-test/test.nljson /user/smenadjlia/data-test/seq

# yarn jar target/ClashRoyale-0.0.1.jar 

# for i in {1..12}; do
#     yarn jar target/ClashRoyale-0.0.1.jar /user/smenadjlia/data/GameData /user/smenadjlia/data/deck-stats/m-$i 100 m-$i 1
# done

# for i in {1..52}; do
#     yarn jar target/ClashRoyale-0.0.1.jar /user/smenadjlia/data/GameData /user/smenadjlia/data/deck-stats/w-$i 100 w-$i 1
# done


for i in {1..12}; do
    for j in {1..6}; do
        yarn jar target/ClashRoyale-0.0.1.jar /user/smenadjlia/data/deck-stats/m-$i/part-r-00000 /user/smenadjlia/data/deck-stats/m-$i/s$j 50 m-$i $j
        echo "month no $j"
    done
done

for i in {1..52}; do
    for j in {1..6}; do
        yarn jar target/ClashRoyale-0.0.1.jar /user/smenadjlia/data/deck-stats/w-$i/part-r-00000 /user/smenadjlia/data/deck-stats/w-$i/s$j 50 w-$i $j
        echo "week no $j"
    done
done

# all data 

# yarn jar target/ClashRoyale-0.0.1.jar /user/smenadjlia/data/GameData /user/smenadjlia/data/deck-stats/all 50 a-0 1

# for j in {1..6}; do
#     yarn jar target/ClashRoyale-0.0.1.jar /user/smenadjlia/data/all/part-r-00000  /user/smenadjlia/data/deck-stats/all/s$j 50 a-0 1
# done

# hdfs dfs -get /user/smenadjlia/data-test/res* data/results/ 

# hdfs dfs -get /user/smenadjlia/data-test/res-all/part-r-00000 data/

# hdfs dfs -rm /user/smenadjlia/data/StatsData
# hdfs dfs -mv /user/smenadjlia/data-test/res-all/part-r-00000 /user/smenadjlia/data/StatsData
