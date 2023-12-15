package num_plays;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.stream.Collectors;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class NumPlays {

    public static class PlaysMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private TreeMap<String, Integer> plays = new TreeMap<String, Integer>();

        private String getMonthYear(LocalDateTime date) {
            return date.getMonthValue() + "";
        }

        private String getWeekYear(LocalDateTime date) {
            return date.get(WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear()) + "";
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("K"));
            String seed = conf.get("seed");

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(value.toString());

            if (!rootNode.has("date") || !rootNode.has("player") ||
                    !rootNode.has("player2") || !rootNode.has("cards") ||
                    !rootNode.has("cards2") || !rootNode.has("crown") ||
                    !rootNode.has("crown2"))

                return;

            String dateString = rootNode.get("date").asText();
            LocalDateTime date = LocalDateTime.parse(dateString, DateTimeFormatter.ISO_DATE_TIME);

            String[] seedSplit = seed.split("-");
            String seedLevel = seedSplit[0];
            String seedNumber = seedSplit[1];

            switch (seedLevel) {
                case "w":
                    if (!Objects.equals(seedNumber, getWeekYear(date)))
                        return;
                    break;
                case "m":
                    if (!Objects.equals(seedNumber, getMonthYear(date)))
                        return;
                    break;
                default:
                    break;
            }

            String cards = rootNode.get("cards").asText();
            String cards2 = rootNode.get("cards2").asText();

            int sum = 1;
            if (plays.containsKey(cards))
                sum += plays.get(cards);

            if (!cards.isEmpty())
                plays.put(cards, sum);

            sum = 1;
            if (plays.containsKey(cards2))
                sum += plays.get(cards2);

            if (!cards2.isEmpty())
                plays.put(cards2, sum);

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Map.Entry<String, Integer> entry : plays.descendingMap().entrySet()) {
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }

        }
    }

    public static class PlaysReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private TreeMap<Integer, String> plays = new TreeMap<Integer, String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("K"));
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            plays.put(sum, key.toString());

            if (plays.size() > k) {
                plays.remove(plays.firstKey());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Map.Entry<Integer, String> entry : plays.descendingMap().entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    public static void runJob(String arg1, String arg2, String arg3, String arg4) throws Exception {

        // if (args.length != 4) {
        // System.out.println(
        // "Invalid usage : you need to provide 4 arguments : <input_file> <output_file>
        // <K> <seed-number>.");
        // System.exit(1);/user/smenadjlia/data-test/res-all
        // }

        Configuration conf = new Configuration();
        conf.set("K", arg3);
        conf.set("seed", arg4);
        Job job = Job.getInstance(conf, "NumPlays");

        job.setNumReduceTasks(1);
        job.setJarByClass(NumPlays.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(PlaysMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(PlaysReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, new Path(arg1));
        TextOutputFormat.setOutputPath(job, new Path(arg2));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
