package average_difference;

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

public class AverageDifference {

    public static class AvgMapper extends Mapper<LongWritable, Text, Text, Text> {
        private TreeMap<String, ArrayList<Double>> levels = new TreeMap<String, ArrayList<Double>>();

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
                    !rootNode.has("crown2") || !rootNode.has("clanTr") || !rootNode.has("clanTr2")
                    || !rootNode.has("deck") || !rootNode.has("deck2"))

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

            String winnerCards = new String();

            String cards = rootNode.get("cards").asText();
            String cards2 = rootNode.get("cards2").asText();
            int crown = rootNode.get("crown").asInt();
            int crown2 = rootNode.get("crown2").asInt();
            double deck = rootNode.get("deck").asDouble();
            double deck2 = rootNode.get("deck2").asDouble();
            Double levelDiff = new Double(0);

            if (crown > crown2) {
                levelDiff = deck - deck2;
                winnerCards = cards;
            } else if (crown2 > crown) {
                levelDiff = deck2 - deck;
                winnerCards = cards2;
            } else
                return;

            if (!winnerCards.isEmpty()) {
                levels.put(winnerCards, new ArrayList<Double>());
            }

            levels.get(winnerCards).add(levelDiff);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Map.Entry<String, ArrayList<Double>> entry : levels.descendingMap().entrySet()) {
                String diffs = entry.getValue().stream().map(Object::toString).collect(Collectors.joining(", "));
                context.write(new Text(entry.getKey()), new Text(diffs));
            }

        }
    }

    public static class AvgReducer extends Reducer<Text, Text, Text, Text> {

        private TreeMap<String, String> levels = new TreeMap<String, String>();

        public void reduce(Text key, Iterable<String> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("K"));

            Double avg = 0.0;
            int divider = 0;

            for (String value : values) {
                String[] line = value.split(", ");
                divider += line.length;

                for (int i = 0; i < line.length; i++) {
                    avg += Double.valueOf(line[i]);
                }
            }

            avg = avg / divider;
            levels.put(avg.toString(), key.toString());
            if (levels.size() > k) {
                levels.remove(levels.firstKey());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Integer k = Integer.parseInt(conf.get("K"));

            if (k == 10) {
                context.write(new Text("wrong"), new Text(k.toString() + ""));
                return;
            }

            // else
            //     for (Map.Entry<String, String> entry : levels.entrySet()) {
            //         context.write(new Text(entry.getValue()), new Text(entry.getKey()));
            //     }
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
        Job job = Job.getInstance(conf, "AverageDifference");

        job.setNumReduceTasks(1);
        job.setJarByClass(AverageDifference.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(AvgMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path(arg1));
        TextOutputFormat.setOutputPath(job, new Path(arg2));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
