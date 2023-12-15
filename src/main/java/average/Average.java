package average;

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

import java.math.BigDecimal;
import java.math.RoundingMode; 
import java.lang.Math;

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

public class Average {

    public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {
        private TreeMap<String, String> avg = new TreeMap<String, String>();

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
            Double deck = rootNode.get("deck").asDouble();
            Double deck2 = rootNode.get("deck2").asDouble();
            Double levelDiff = Math.abs(deck-deck2);



            if (crown > crown2) {
                winnerCards = cards;
            } else if (crown2 > crown) {
                winnerCards = cards2;
            } else
                return;
            

            Integer nb = 1;

            if (avg.containsKey(winnerCards)){
                String split[] = avg.get(winnerCards).split("-");
                levelDiff += Double.parseDouble(split[0]);
                nb += Integer.parseInt(split[1]);
            }
            
            String avgNb = levelDiff.toString() + "-" + nb.toString();

            if (!winnerCards.isEmpty())
                avg.put(winnerCards, avgNb);


        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Map.Entry<String, String> entry : avg.descendingMap().entrySet()) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue()));
            }

        }
    }

    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {

        private TreeMap<Double, String> avg = new TreeMap<Double, String>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("K"));
            int nb = 0;
            Double levelDiff = new Double(0);
            for (Text value : values) {

                String split[] = value.toString().split("-");
                levelDiff += Double.parseDouble(split[0]);
                nb += Integer.parseInt(split[1]);
            }

            avg.put(levelDiff/nb, key.toString());
            if (avg.size() > k) {
                    avg.remove(avg.firstKey());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Map.Entry<Double, String> entry : avg.descendingMap().entrySet()) {
                context.write(new Text(entry.getValue()), new Text(String.format("%.4f", entry.getKey())));
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
        Job job = Job.getInstance(conf, "Average");

        job.setNumReduceTasks(1);
        job.setJarByClass(Average.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(AverageMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path(arg1));
        TextOutputFormat.setOutputPath(job, new Path(arg2));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
