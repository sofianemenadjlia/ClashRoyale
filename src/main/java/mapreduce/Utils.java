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
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;
import scala.Tuple3;

import deck.*;;

public class Utils {

    public static final byte[] STATS_FAMILY = Bytes.toBytes("stats");
    public static final String TABLE_NAME = "fmessaoud:ClashTable";

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

    public static <T> void generateCombinations(List<T> list, int length, int startIndex, List<T> current, List<List<T>> result) {
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

    public static List<List<List<DeckStats>>> exportJson(JavaPairRDD<Integer, Iterable<DeckStats>> groupedBySize, int k, String jsonDir){
        
        ObjectMapper mapper = new ObjectMapper();
        List<List<List<DeckStats>>> allSizeTopKCombinations = new ArrayList<>();

        List<Function<DeckStats, Comparable>> sortFunctions = new ArrayList<>();
            sortFunctions.add(Utils::compareByWins);
            sortFunctions.add(Utils::compareByRatio);
            sortFunctions.add(Utils::compareByUses);
            sortFunctions.add(Utils::compareByNbPlayers);
            sortFunctions.add(Utils::compareByClanLevel);
            sortFunctions.add(Utils::compareByAverageLevel);
            
        for (int size = 1; size <= 8; size++) {

            List<List<DeckStats>> allStatsTopK = new ArrayList<>();
            final int currentSize = size; 

            for (Function<DeckStats, Comparable> sortFunction : sortFunctions) {
                List<DeckStats> topKDecks = groupedBySize
                    .filter(entry -> entry._1() == currentSize) 
                    .flatMapValues(Iterable::iterator)
                    .map(x -> x._2)
                    .sortBy(sortFunction, false, 1)
                    .take(k);
                allStatsTopK.add(topKDecks);
            }
            allSizeTopKCombinations.add(allStatsTopK);
        }

        try {
            mapper.writeValue(new File(jsonDir + ".json"), allSizeTopKCombinations);   
        } catch (IOException e) {
            e.printStackTrace();
        }

        return allSizeTopKCombinations;
    }



    public static void createTable(Connection connect) {
        try {
            final Admin admin = connect.getAdmin();
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            tableDescriptor.addFamily(new HColumnDescriptor(STATS_FAMILY));
            if (admin.tableExists(tableDescriptor.getTableName())) {
                // admin.disableTable(tableDescriptor.getTableName());
                // admin.deleteTable(tableDescriptor.getTableName());
                return;
            }
            admin.createTable(tableDescriptor);
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    public static Tuple2<ImmutableBytesWritable, Put> prepareForHbase(Tuple2<String,String> x) {

        Put put = new Put(Bytes.toBytes(x._1));
        put.addColumn(STATS_FAMILY, Bytes.toBytes("stats"), Bytes.toBytes(x._2));
        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
    }



}
