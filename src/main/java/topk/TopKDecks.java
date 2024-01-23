package topk;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

import deck.Deck;
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

public class TopKDecks {

    public static void main(String[] args) {
        try {
            String stats = "/user/smenadjlia/data/stats";
            int k = 10;
            ObjectMapper mapper = new ObjectMapper();

            // Initialize Spark Context and Spark Session
            SparkConf conf = new SparkConf().setAppName("TopKDecks");
            JavaSparkContext sc = new JavaSparkContext(conf);
            SparkSession spark = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();

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


            // Loop through stats and compute topK for each
            int i = 1;
            for (Function<Tuple2<String, Tuple2<Tuple3<Integer, Double, Integer>, Tuple3<Integer, Integer, Double>>>, Comparable> sortFunction : sortFunctions) {
                List<Tuple2<String, Tuple2<Tuple3<Integer, Double, Integer>, Tuple3<Integer, Integer, Double>>>>
                topKDecks = deckAttributes.sortBy(sortFunction, false, 1).take(k);
                
                List<Deck> topK = new ArrayList<>();
                
                for (Tuple2<String, Tuple2<Tuple3<Integer, Double, Integer>, Tuple3<Integer, Integer, Double>>> deck: topKDecks){
                    Deck d = new Deck(deck._1(), deck._2()._1()._1(), deck._2()._1()._2(), deck._2()._1()._3(), deck._2()._2()._1(), deck._2()._2()._2(), deck._2()._2()._3());
                    // d.setNb
                    topK.add(d);
                }
                mapper.writeValue(new File("topKDecksBy_s" + i + ".json"), topK);
                
                ++i;
            }

            sc.close();
            spark.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
