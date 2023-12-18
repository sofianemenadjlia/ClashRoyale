package topk;

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

public class TopK {
    public static class TopkMapper extends Mapper<Deck, NullWritable, Text, Deck> {

        public void map(Deck key, NullWritable value, Context context) throws IOException, InterruptedException {

            context.write(new Text(key.getId()), key);
        }

    }

    public static class TopKReducer extends Reducer<Text, Deck, Text, Text> {

        private TreeMap<Double, String> topk = new TreeMap<Double, String>();

        public void reduce(Text key, Iterable<Deck> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));
            int stat = Integer.parseInt(conf.get("stat"));

            if (stat >= 6 || stat < 0 || k < 1)
                return;

            for (Deck value : values) {

                List<Double> stats = new ArrayList<Double>();
                stats.add((double) value.getWins());
                stats.add((double) value.getRatio());
                stats.add((double) value.getUses());
                stats.add((double) value.getNbPlayers());
                stats.add((double) value.getClanLevel());
                stats.add((double) value.getAverageLevel());

                topk.put(stats.get(stat), value.toJson());
                if (topk.size() > k) {
                    topk.remove(topk.firstKey());
                }
                break;
            }

        }

        protected void cleanup(Context context) throws IOException,
                InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));

            context.write(new Text("["), new Text(""));

            for (Map.Entry<Double, String> entry : topk.descendingMap().entrySet()) {

                // remove last element comma from json file
                k--;
                if (k == 0)
                    context.write(new Text(entry.getValue()), new Text(""));
                else
                    context.write(new Text(entry.getValue()), new Text(","));
            }
            context.write(new Text("]"), new Text(""));
        }

    }

    public static void runJob(String arg1, String arg2, String arg3, String arg4) throws Exception {

        Configuration conf = new Configuration();
        conf.set("k", arg3);
        conf.set("stat", arg4);
        // conf.set("seed", arg4);
        Job job = Job.getInstance(conf, "TopK");

        job.setNumReduceTasks(1);
        job.setJarByClass(TopK.class);

        // job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(TopkMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Deck.class);

        job.setReducerClass(TopKReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path(arg1));
        TextOutputFormat.setOutputPath(job, new Path(arg2));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
