// import mapreduce.Deck;
// import mapreduce.DeckStats;
// import mapreduce.ClashGame;
// import mapreduce.Filter;
// import mapreduce.Stats;
// import mapreduce.TopK;
// import mapreduce.TopKDecks;
// import mapreduce.TopKCombinations;
// import mapreduce.Utils;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;

public class Main {

    public static void main(String[] args) throws Exception {


            // .setMaster("yarn")
            // .set("spark.executor.memory", "48g")
            // .set("spark.driver.memory", "48g")
            // .set("spark.executor.instances", "8")
            // .set("spark.executor.cores", "8");

        // String raw = "/user/auber/data_ple/clashroyale/gdc_battles.nljson";
        // String gameData = "/user/smenadjlia/data/GameData";
        // String games = "/user/smenadjlia/data/games";
        // String stats = "/user/smenadjlia/data/stats";
        // String statsData = "/user/smenadjlia/data/StatsData";
        // String output = "/user/smenadjlia/data-test/res-all";
        // String k = "10";
        // String seed = "a-0";
        // String stat = "0";

        // Filter.runJob(raw, output);
        // Stats.runJob(games, output, seed);
        // TopK.runJob(stats, output, k, stat);
        
        // for (int m = 1; m < 12; m++) {
        // String monthSeed = "m-" + m;
        // String monthOutput = "/user/smenadjlia/data/deck-stats/" + monthSeed;
        // Stats.runJob(gameData, monthOutput, monthSeed);
        // }


        // String inputFile = args[0];
        // String outputFile = args[1];
        // String k = args[2];
        // String seed = args[3];
        // String stat = args[4];

        // Stats.runJob(inputFile, outputFile, seed);
        // Stats.runJob(inputFile, outputFile, k, stat);
    }
}