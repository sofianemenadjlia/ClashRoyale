package runner;

import filter.Filter;
import clash_game.ClashGame;
import deck.Deck;
import stats.Stats;
import topk.TopK;

public class Main {

    public static void main(String[] args) throws Exception {

        // String raw = "/user/auber/data_ple/clashroyale/gdc_battles.nljson";
        // String gameData = "/user/smenadjlia/data/GameData";
        // String statsData = "/user/smenadjlia/data/StatsData";
        // String output = "/user/smenadjlia/data-test/res-all";
        // String k = "50";
        // String seed = "w-52";
        // String stat = "3";

        // Filter.runJob(raw, output);

        // for (int m = 1; m < 12; m++) {
        // String monthSeed = "m-" + m;
        // String monthOutput = "/user/smenadjlia/data/deck-stats/" + monthSeed;
        // Stats.runJob(gameData, monthOutput, monthSeed);
        // }

        // Stats.runJob(gameData, output, seed);

        String inputFile = args[0];
        String outputFile = args[1];
        String k = args[2];
        String seed = args[3];
        String stat = args[4];

        // Stats.runJob(inputFile, outputFile, seed);
        TopK.runJob(inputFile, outputFile, k, stat);
    }
}