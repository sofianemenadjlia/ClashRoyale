package topk;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
// import org.apache.hadoop.hbase.HBaseConfiguration;
// import org.apache.hadoop.hbase.TableName;
// import org.apache.hadoop.hbase.client.*;
// import org.apache.hadoop.hbase.util.Bytes;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Table;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.stream.StreamSupport;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import deck.Deck;
import deck.DeckStats;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;

public class TopKCombinations {

    public static void main(String[] args) {
            String stats = "/user/smenadjlia/data/stats";
            String statsM6 = "/user/smenadjlia/data/statsM6";
            String statPath = "sparkJson/";
            int k = 10;
            ObjectMapper mapper = new ObjectMapper();

            // Initialize Spark Context and Spark Session
            SparkConf conf = new SparkConf().setAppName("TopKCombos");
            JavaSparkContext sc = new JavaSparkContext(conf);

            // Read data from a SequenceFile or other source
            JavaPairRDD<Deck, NullWritable> sequenceData = sc.sequenceFile(statsM6, Deck.class, NullWritable.class);

            // Convert to RDD of decks
            JavaRDD<Deck> decks = sequenceData.keys();

            // Generate and process combinations
            JavaPairRDD<String, DeckStats> combinationStats = decks.flatMapToPair(deck -> {
                List<Tuple2<String, DeckStats>> results = new ArrayList<>();
                
                Set<String> combinationSet = new HashSet<>();

                String deckId = deck.getId();
                List<String> cardIds = parseDeckId(deckId); // Implement this method to parse deckId into card IDs

                for (int i = 1; i <= 8; i++) {
                    for (List<String> combination : combinations(cardIds, i)) {
                        String combinationKey = processStrings(combination); // Create a unique sorted key for the combination
                        if (combinationSet.add(combinationKey))
                            results.add(new Tuple2<>(combinationKey, new DeckStats(deck, combinationKey))); // DeckStats is a class holding deck's stats
                    }
                }
                return results.iterator();
            });

            // Aggregate stats for each combination
            JavaPairRDD<String, DeckStats> aggregatedStats = combinationStats.reduceByKey((stats1, stats2) -> stats1.combine(stats2)).mapValues(stat -> {
                if (stat.getUses() != 0)
                    stat.setRatio((double) stat.getWins() / stat.getUses());
                else
                    stat.setRatio(0);
                
                if (stat.getWins() != 0) 
                    stat.setAverageLevel((double) stat.getAverageLevel() / stat.getWins());
                else 
                    stat.setAverageLevel(0);
                
                return stat;
            });

            List<Function<DeckStats, Comparable>> sortFunctions = new ArrayList<>();
            sortFunctions.add(TopKCombinations::compareByWins);
            sortFunctions.add(TopKCombinations::compareByRatio);
            sortFunctions.add(TopKCombinations::compareByUses);
            sortFunctions.add(TopKCombinations::compareByNbPlayers);
            sortFunctions.add(TopKCombinations::compareByClanLevel);
            sortFunctions.add(TopKCombinations::compareByAverageLevel);

            // Key is a tuple of (combinationSize, combinationKey)
            JavaPairRDD<Tuple2<Integer, String>, DeckStats> modifiedStats = aggregatedStats
                .mapToPair(entry -> {
                    int size = entry._1().split("-").length; // Determine the combination size
                    Tuple2<Integer, String> newKey = new Tuple2<>(size, entry._1());
                    return new Tuple2<>(newKey, entry._2());
                });

            // Group by the combination size (which is the first element of the tuple key)
            JavaPairRDD<Integer, Iterable<DeckStats>> groupedBySize = modifiedStats
                .mapToPair(entry -> new Tuple2<>(entry._1()._1(), entry._2())).groupByKey();

            List<List<List<DeckStats>>> allSizeTopKCombinations = new ArrayList<>();

            for (int size = 1; size <= 8; size++) {
                final int currentSize = size; // Final variable for use in lambda
                List<List<DeckStats>> allStatsTopK = new ArrayList<>();
                for (Function<DeckStats, Comparable> sortFunction : sortFunctions) {
                    List<DeckStats> topKDecks = groupedBySize
                        .filter(entry -> entry._1() == currentSize) // Use final variable here
                        .flatMapValues(Iterable::iterator)
                        .map(x -> x._2)
                        .sortBy(sortFunction, false, 1)
                        .take(k);

                    allStatsTopK.add(topKDecks);
                }
                allSizeTopKCombinations.add(allStatsTopK);
            }

            try {
                mapper.writeValue(new File("TopKCombinationsBySize.json"), allSizeTopKCombinations);   
            } catch (IOException e) {
                e.printStackTrace();
            }

            sc.close();

        
    }
    
    public static int compareByWins(DeckStats stats) {
        return stats.getWins();
    }

    // Static method for comparing by uses
    public static int compareByUses(DeckStats stats) {
        return stats.getUses();
    }

    // Static method for comparing by ratio
    public static double compareByRatio(DeckStats stats) {
        return stats.getRatio();
    }

    // Static method for comparing by number of players
    public static int compareByNbPlayers(DeckStats stats) {
        return stats.getNbPlayers();
    }

    // Static method for comparing by clan level
    public static int compareByClanLevel(DeckStats stats) {
        return stats.getClanLevel();
    }

    // Static method for comparing by average level
    public static double compareByAverageLevel(DeckStats stats) {
        return stats.getAverageLevel();
    }

    public static String processStrings(List<String> cards) {
        // Sort the hex strings
        cards.sort((a, b) -> Integer.compare(Integer.parseInt(a, 16), Integer.parseInt(b, 16)));

        // Merge them back into a single string
        return String.join("-", cards);
    }

    // Method to parse deck ID into card IDs
    public static List<String> parseDeckId(String deckId) {
        List<String> cardIds = new ArrayList<>();
        for (int i = 0; i < deckId.length(); i += 2) {
            cardIds.add(deckId.substring(i, i + 2));
        }
        return cardIds;
    }

    // Method to generate combinations of a certain length
    public static <T> List<List<T>> combinations(List<T> list, int length) {
        List<List<T>> result = new ArrayList<>();
        generateCombinations(list, length, 0, new ArrayList<T>(), result);
        return result;
    }

    private static <T> void generateCombinations(List<T> list, int length, int startIndex, List<T> current, List<List<T>> result) {
        if (current.size() == length) {
            result.add(new ArrayList<>(current));
            return;
        }

        for (int i = startIndex; i < list.size(); i++) {
            current.add(list.get(i));
            generateCombinations(list, length, i + 1, current, result);
            current.remove(current.size() - 1);
        }
    }

}
