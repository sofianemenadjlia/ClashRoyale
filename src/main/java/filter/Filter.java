package filter;

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

import clash_game.ClashGame;

public class Filter {

    public static class FilterMapper extends Mapper<LongWritable, Text, ClashGame, NullWritable> {

        private int getMonthYear(LocalDateTime date) {
            return date.getMonthValue();
        }

        private int getWeekYear(LocalDateTime date) {
            return date.get(WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear());
        }

        public static String sortAndMergeHexStrings(String input) {
            // Check if the length of the input string is valid
            if (input.length() != 16) {
                return "";
            }

            // Determine the length of each substring
            int substringLength = input.length() / 8;

            // Split the input string into 8 substrings
            String[] hexStrings = new String[8];
            for (int i = 0; i < 8; i++) {
                hexStrings[i] = input.substring(i * substringLength, (i + 1) * substringLength);
            }

            // Sort the hex strings
            Arrays.sort(hexStrings, (s1, s2) -> Long.compare(Long.parseLong(s1, 16), Long.parseLong(s2, 16)));

            // Merge the sorted hex strings back into one string
            return String.join("", hexStrings);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            ObjectMapper objectMapper = new ObjectMapper();
            ObjectMapper outputMapper = new ObjectMapper();

            JsonNode rootNode = objectMapper.readTree(value.toString());

            if (!rootNode.has("date") || !rootNode.has("player") ||
                    !rootNode.has("player2") || !rootNode.has("cards") ||
                    !rootNode.has("cards2") || !rootNode.has("crown") ||
                    !rootNode.has("crown2") || !rootNode.has("clanTr") || !rootNode.has("clanTr2")
                    || !rootNode.has("deck") || !rootNode.has("deck2"))

                return;

            String dateString = rootNode.get("date").asText();
            String player = rootNode.get("player").asText();
            String player2 = rootNode.get("player2").asText();
            String cards = rootNode.get("cards").asText();
            String cards2 = rootNode.get("cards2").asText();
            int crown = rootNode.get("crown").asInt();
            int crown2 = rootNode.get("crown2").asInt();
            int clanTr = rootNode.get("clanTr").asInt();
            int clanTr2 = rootNode.get("clanTr2").asInt();
            Double deck = rootNode.get("deck").asDouble();
            Double deck2 = rootNode.get("deck2").asDouble();

            cards = sortAndMergeHexStrings(cards);
            cards = sortAndMergeHexStrings(cards2);

            if (dateString.isEmpty() || player.isEmpty() || player2.isEmpty() ||
                    cards.isEmpty() || cards.isEmpty())

                return;

            LocalDateTime date = LocalDateTime.parse(dateString, DateTimeFormatter.ISO_DATE_TIME);
            int month = getMonthYear(date);
            int week = getWeekYear(date);

            ClashGame game = new ClashGame();
            game.setMonth(month);
            game.setWeek(week);
            game.setPlayer(player);
            game.setPlayer2(player2);
            game.setCards(cards);
            game.setCards2(cards2);
            game.setCrown(crown);
            game.setCrown2(crown2);
            game.setClanTr(clanTr);
            game.setClanTr2(clanTr2);
            game.setDeck(deck);
            game.setDeck2(deck2);

            context.write(game, NullWritable.get());

        }
    }

    public static class FilterReducer extends Reducer<ClashGame, NullWritable, ClashGame, NullWritable> {

        public void reduce(ClashGame key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            context.write(key, NullWritable.get());

        }

    }

    public static void runJob(String arg1, String arg2) throws Exception {

        // if (args.length != 4) {
        // System.out.println(
        // "Invalid usage : you need to provide 4 arguments : <input_file> <output_file>
        // <K> <seed-number>.");
        // System.exit(1);/user/smenadjlia/data-test/res-all
        // }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Filter");

        job.setNumReduceTasks(1);
        job.setJarByClass(Filter.class);

        job.setInputFormatClass(TextInputFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(FilterMapper.class);
        job.setMapOutputKeyClass(ClashGame.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(ClashGame.class);
        job.setOutputValueClass(NullWritable.class);

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        TextInputFormat.addInputPath(job, new Path(arg1));
        TextOutputFormat.setOutputPath(job, new Path(arg2));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
