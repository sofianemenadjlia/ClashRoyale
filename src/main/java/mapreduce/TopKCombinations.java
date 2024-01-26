package mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;

import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
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


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

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

import deck.*;

public class TopKCombinations {

    public static final byte[] STATS_FAMILY = Bytes.toBytes("stats");
    public static final String TABLE_NAME = "fmessaoud:clashgametable";

    public static void runJob(JavaSparkContext sc, String statsInput, int k, String jsonDir, String filename) {

            JavaRDD<Deck> decks = sc.sequenceFile(statsInput, Deck.class, NullWritable.class).keys();

            // Generate and process combinations
            JavaPairRDD<String, DeckStats> combinationStats = decks.flatMapToPair(deck -> {
                List<Tuple2<String, DeckStats>> results = new ArrayList<>();
                
                Set<String> combinationSet = new HashSet<>();

                String deckId = deck.getId();
                List<String> cardIds = Utils.parseDeckId(deckId);
                for (int i = 1; i <= 8; i++) {
                    for (List<String> combination : Utils.combinations(cardIds, i)) {
                        String combinationKey = Utils.processStrings(combination); // Create a unique sorted key for the combination
                        if (combinationSet.add(combinationKey))
                            results.add(new Tuple2<>(combinationKey, new DeckStats(deck, combinationKey))); 
                    }
                }
                return results.iterator();
            });

            // Aggregate stats using reduceByKey for each combination
            JavaPairRDD<String, DeckStats> aggregatedStats = combinationStats.reduceByKey((stats1, stats2) -> stats1.combine(stats2)).mapValues(stat -> {

                stat.setRatio((double) stat.getWins() / stat.getUses());
                
                if (stat.getWins() != 0) stat.setAverageLevel((double) stat.getAverageLevel() / stat.getWins());
                
                else stat.setAverageLevel(0);
                
                return stat;
            });

            // Key is a tuple of (combinationSize, combinationKey)
            JavaPairRDD<Tuple2<Integer, String>, DeckStats> modifiedStats = aggregatedStats.mapToPair(entry -> {
                    int size = entry._1().split("-").length; // Determine the combination size
                    Tuple2<Integer, String> newKey = new Tuple2<>(size, entry._1());
                    return new Tuple2<>(newKey, entry._2());
            });

            // Group by the combination size
            JavaPairRDD<Integer, Iterable<DeckStats>> groupedBySize = modifiedStats
                .mapToPair(entry -> new Tuple2<>(entry._1()._1(), entry._2())).groupByKey();

            // export to json file 
            List<List<List<DeckStats>>> topkJson = Utils.extractKExportJson(groupedBySize, k, jsonDir);

            // store in database 
            try{
                Configuration conf_hbase = Utils.init();
                Connection connection = ConnectionFactory.createConnection(conf_hbase);

                Utils.createTable(connection);
                Utils.exportToHbase(sc, filename, topkJson, conf_hbase);

            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
            sc.close();
    }

    public static void main(String[] args){

        int k = 100;
        SparkConf conf = new SparkConf().setAppName("TopKCombos");

        JavaSparkContext sc = new JavaSparkContext(conf);
        String username = "fmessaoud";
        String filename = args[0];
        String jsonDir = "sparkJson/" + filename;
        String statsInput = "/user/" + username + "/data/statseqfiles/" + filename;
        
        runJob(sc, statsInput, k, jsonDir, filename);
    }
}
