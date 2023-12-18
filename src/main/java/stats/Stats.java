package stats;

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

import clash_game.ClashGame;
import deck.Deck;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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

import clash_game.ClashGame;
import deck.Deck;

public class Stats {

    public static class StatsMapper extends Mapper<ClashGame, NullWritable, Text, Deck> {

        public void map(ClashGame key, NullWritable value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String seed = conf.get("seed");

            String[] seedSplit = seed.split("-");
            String seedLevel = seedSplit[0];
            int seedNumber = Integer.parseInt(seedSplit[1]);

            if (seedLevel.equals("m"))
                if (key.getMonth() != seedNumber)
                    return;

            if (seedLevel.equals("w"))
                if (key.getWeek() != seedNumber)
                    return;

            int winner = 0;

            if (key.getCrown() > key.getCrown2())
                winner = 1;
            else if (key.getCrown2() > key.getCrown())
                winner = 2;

            Deck deck1 = new Deck(key.getCards(), winner == 1 ? 1 : 0, 1.0, 1, key.getPlayer(),
                    winner == 1 ? key.getClanTr() : 0, winner == 1 ? key.getDeck() - key.getDeck2() : 0);

            Deck deck2 = new Deck(key.getCards2(), winner == 2 ? 1 : 0, 1.0, 1, key.getPlayer2(),
                    winner == 2 ? key.getClanTr2() : 0, winner == 2 ? key.getDeck2() - key.getDeck() : 0);

            context.write(new Text(deck1.getId()), deck1);
            context.write(new Text(deck2.getId()), deck2);
        }
    }

    public static class StatsCombiner extends Reducer<Text, Deck, Text, Deck> {

        @Override
        public void reduce(Text key, Iterable<Deck> values, Context context) throws IOException, InterruptedException {
            Deck finalDeck = new Deck(key.toString());

            for (Deck value : values) {
                finalDeck.setWins(finalDeck.getWins() + value.getWins());
                finalDeck.setUses(finalDeck.getUses() + value.getUses());

                for (String player : value.getPlayers()) {
                    finalDeck.addPlayer(player);
                }

                finalDeck.setClanLevel(Math.max(value.getClanLevel(), finalDeck.getClanLevel()));
                finalDeck.setAverageLevel(finalDeck.getAverageLevel() + value.getAverageLevel());
            }

            if (finalDeck.getUses() > 10)
                context.write(new Text(finalDeck.getId()), finalDeck);

        }
    }

    public static class StatsReducer extends Reducer<Text, Deck, Deck, NullWritable> {

        public void reduce(Text key, Iterable<Deck> values, Context context)
                throws IOException, InterruptedException {

            Deck finalDeck = new Deck(key.toString());

            for (Deck value : values) {
                finalDeck.setWins(finalDeck.getWins() + value.getWins());
                finalDeck.setUses(finalDeck.getUses() + value.getUses());

                for (String player : value.getPlayers()) {
                    finalDeck.addPlayer(player);
                }

                finalDeck.setClanLevel(Math.max(value.getClanLevel(), finalDeck.getClanLevel()));
                finalDeck.setAverageLevel(finalDeck.getAverageLevel() + value.getAverageLevel());
            }
            double avg = finalDeck.getWins() == 0 ? 0 : finalDeck.getAverageLevel() / finalDeck.getWins();
            finalDeck.setAverageLevel(avg);

            finalDeck.setRatio((double) finalDeck.getWins() / finalDeck.getUses());

            context.write(finalDeck, NullWritable.get());

        }

    }

    public static void runJob(String arg1, String arg2, String arg3) throws Exception {

        Configuration conf = new Configuration();
        conf.set("seed", arg3);

        Job job = Job.getInstance(conf, "Stats");

        job.setNumReduceTasks(1);
        job.setJarByClass(Stats.class);

        // job.setInputFormatClass(TextInputFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(StatsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Deck.class);

        job.setCombinerClass(StatsCombiner.class);

        job.setReducerClass(StatsReducer.class);
        job.setOutputKeyClass(Deck.class);
        job.setOutputValueClass(NullWritable.class);

        TextInputFormat.addInputPath(job, new Path(arg1));
        TextOutputFormat.setOutputPath(job, new Path(arg2));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
