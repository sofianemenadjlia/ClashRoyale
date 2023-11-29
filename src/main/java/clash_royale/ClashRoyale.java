package clash_royale;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;

import org.apache.hadoop.conf.Configuration;
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

public class ClashRoyale {

    public static class clashMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private TreeMap<String, Integer> victories = new TreeMap<String, Integer>();

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
            String winnerCards = new String();

            if (!rootNode.has("date") || !rootNode.has("player") ||
                    !rootNode.has("player2") || !rootNode.has("cards") ||
                    !rootNode.has("cards2") || !rootNode.has("crown") ||
                    !rootNode.has("crown2") || !rootNode.has("win"))

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
            String crown = rootNode.get("crown").asText();
            String crown2 = rootNode.get("crown2").asText();
            String win = rootNode.get("win").asText();

            try {
                int crowns = Integer.parseInt(crown);
                int crowns2 = Integer.parseInt(crown2);
                if (crowns > crowns2)
                    winnerCards = cards;
                else if (crowns2 > crowns)
                    winnerCards = cards2;
                else
                    return;

            } catch (Exception e) {
                return;
            }

            int sum = 1;
            if (victories.containsKey(winnerCards))
                sum += victories.get(winnerCards);

            if (!winnerCards.isEmpty())
                victories.put(winnerCards, sum);

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : victories.descendingMap().entrySet()) {
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    public static class clashReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private TreeMap<Integer, String> victories = new TreeMap<Integer, String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("K"));
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            victories.put(sum, key.toString());

            if (victories.size() > k) {
                victories.remove(victories.firstKey());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            // context.write(new Text("Topk victories for " + " no :"), new
            // IntWritable(monthInt));
            for (Map.Entry<Integer, String> entry : victories.descendingMap().entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println(
                    "Invalid usage : you need to provide 4 arguments : <input_file> <output_file> <K> <seed-number>.");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("K", args[2]);
        conf.set("seed", args[3]);
        Job job = Job.getInstance(conf, "ClashRoyale");

        job.setNumReduceTasks(1);
        job.setJarByClass(ClashRoyale.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(clashMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(clashReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
