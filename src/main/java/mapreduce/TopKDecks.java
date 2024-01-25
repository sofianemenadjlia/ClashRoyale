package mapreduce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;

import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Table;

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

import deck.*;;

public class TopKDecks {

    public static void main(String[] args) {
        try {
            String stats = "/user/smenadjlia/data/stats";
            int k = 10;
            ObjectMapper mapper = new ObjectMapper();

            // Initialize Spark Context and Spark Session
            SparkConf conf = new SparkConf().setAppName("TopKDecks");
            JavaSparkContext sc = new JavaSparkContext(conf);
            // SparkSession spark = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();

            // Read data from a SequenceFile
            JavaPairRDD<Deck, NullWritable> sequenceData = sc.sequenceFile(stats, Deck.class, NullWritable.class);

            //Map the whole deck
            JavaRDD<Tuple2<String, Tuple2<Tuple3<Integer, Double, Integer>, Tuple3<Integer, Integer, Double>>>> deckAttributes = 
            sequenceData.map(x -> new Tuple2<>(x._1().getId(), new Tuple2<>(
                    new Tuple3<>(x._1().getWins(), x._1().getRatio(), x._1().getUses()),
                    new Tuple3<>(x._1().getNbPlayers(), x._1().getClanLevel(), x._1().getAverageLevel())
                )
            ));

            //Sort functions by attribute
            List<Function<Tuple2<String, Tuple2<Tuple3<Integer, Double, Integer>, Tuple3<Integer, Integer, Double>>>, Comparable>>
                sortFunctions = new ArrayList<>();
                sortFunctions.add(x -> x._2()._1()._1()); // Sort by _1._1
                sortFunctions.add(x -> x._2()._1()._2()); // Sort by _1._2
                sortFunctions.add(x -> x._2()._1()._3()); // Sort by _1._3
                sortFunctions.add(x -> x._2()._2()._1()); // Sort by _2._1
                sortFunctions.add(x -> x._2()._2()._2()); // Sort by _2._2
                sortFunctions.add(x -> x._2()._2()._3()); // Sort by _2._3


            List<List<Deck>> allIterationsTopKDecks = new ArrayList<>();
            int i = 1;
            
            for (Function<Tuple2<String, Tuple2<Tuple3<Integer, Double, Integer>, Tuple3<Integer, Integer, Double>>>, Comparable> sortFunction : sortFunctions) {
                List<Tuple2<String, Tuple2<Tuple3<Integer, Double, Integer>, Tuple3<Integer, Integer, Double>>>>
                    topKDecks = deckAttributes.sortBy(sortFunction, false, 1).take(k);
            
                List<Deck> iterationTopK = new ArrayList<>();
                for (Tuple2<String, Tuple2<Tuple3<Integer, Double, Integer>, Tuple3<Integer, Integer, Double>>> deck : topKDecks) {
                    Deck d = new Deck(deck._1(), deck._2()._1()._1(), deck._2()._1()._2(), deck._2()._1()._3(), deck._2()._2()._1(), deck._2()._2()._2(), deck._2()._2()._3());
                    iterationTopK.add(d);
                }
            
                allIterationsTopKDecks.add(iterationTopK);
                ++i;
            }
            mapper.writeValue(new File("groupedTopKDecksSize8.json"), allIterationsTopKDecks);
            
            sc.close();
            // spark.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}






// List<Function<DeckStats, Comparable>> sortFunctions = new ArrayList<>();
// sortFunctions.add(x -> x.getWins()); // Sort by _1._1
// sortFunctions.add(x -> x.getUses()); // Sort by _1._2
// sortFunctions.add(x -> x.getRatio()); // Sort by _1._3
// sortFunctions.add(x -> x.getNbPlayers()); // Sort by _2._1
// sortFunctions.add(x -> x.getClanLevel()); // Sort by _2._2
// sortFunctions.add(x -> x.getAverageLevel()); // Sort by _2._3

// List<List<List<DeckStats>>> allSizeTopKCombinations = new ArrayList<>();

// // Assuming combination sizes range from 1 to 8
// for (int size = 1; size <= 8; size++) {
    
//     int finalSize = size; // Required for use in lambda expression
//     List<List<DeckStats>> allIterationsTopKCombinations = new ArrayList<>();
    
//     for (Function<DeckStats, Comparable> sortFunction : sortFunctions) {
//         List<DeckStats> topKDecks = aggregatedStats
//             .filter(entry -> {
//                 // Count the number of '-' characters in the key
//                 long count = entry._1().chars().filter(ch -> ch == '-').count();
//                 return count == finalSize - 1; // Number of '-' is one less than the number of cards
//             })
//             .values()
//             .sortBy(sortFunction, false, 1) // Replace with your desired sort function
//             .take(k);

//         allIterationsTopKCombinations.add(topKDecks);
//     }
//     allSizeTopKCombinations.add(allIterationsTopKCombinations);
// }

// List<List<DeckStats>> allIterationsTopKCombinations = new ArrayList<>();
// int i = 1;

// for (Function<DeckStats, Comparable> sortFunction : sortFunctions) {
//     List<DeckStats> topKDecks = aggregatedStats.values().sortBy(sortFunction, false, 1).take(k);

//     allIterationsTopKCombinations.add(topKDecks);
//     ++i;
// }


// ------------------------------------------------------------------------------------------------------------------------------------------



// JavaPairRDD<String, Iterable<DeckStats>> groupedStats = aggregatedStats.groupByKey();

            // // List of comparators for each statistic
            // List<Comparator<DeckStats>> comparators = Arrays.asList(
            //     Comparator.comparingInt(DeckStats::getWins), // For wins
            //     Comparator.comparingDouble(DeckStats::getRatio), // For ratio
            //     Comparator.comparingInt(DeckStats::getUses), // For uses
            //     Comparator.comparingInt(DeckStats::getNbPlayers), // For nbPlayers
            //     Comparator.comparingDouble(DeckStats::getAverageLevel), // For averageLevel
            //     Comparator.comparingInt(DeckStats::getClanLevel) // For clanLevel
            // );

            // Map<String, JavaPairRDD<String, List<DeckStats>>> topkForEachStat = new HashMap<>();

            // String[] statNames = {"Wins", "Ratio", "Uses", "NbPlayers", "AverageLevel", "ClanLevel"};
            // for (int i = 0; i < comparators.size(); i++) {
            //     Comparator<DeckStats> comparator = comparators.get(i);
            //     String statName = statNames[i];

            //     JavaPairRDD<String, List<DeckStats>> topkForStat = groupedStats.mapValues(stat -> StreamSupport.stream(stat.spliterator(), false)
            //             .sorted(comparator.reversed()) // Change or reverse as per sorting order
            //             .limit(k)
            //             .collect(Collectors.toList())
            //     );

            //     topkForEachStat.put(statName, topkForStat);
            // }

            // for (Map.Entry<String, JavaPairRDD<String, List<DeckStats>>> entry : topkForEachStat.entrySet()) {
            //     String statName = entry.getKey();
            //     JavaPairRDD<String, List<DeckStats>> rdd = entry.getValue();

            //     // Collect the RDD data to the driver
            //     List<Tuple2<String, List<DeckStats>>> collectedData = rdd.collect();

            //     // Define the output path for the JSON file
            //     String outputPath = statPath + statName + ".json"; // Modify the path as needed

            //     // Write the JSON string to a file
            //     try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            //         // Write each element in the list as a JSON line
            //         for (Tuple2<String, List<DeckStats>> element : collectedData) {
            //             String jsonString = mapper.writeValueAsString(element);
            //             writer.write(jsonString);
            //             writer.newLine();
            //         }
