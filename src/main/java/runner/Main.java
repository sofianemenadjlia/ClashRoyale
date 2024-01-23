package runner;

import filter.Filter;
import clash_game.ClashGame;
import deck.Deck;
import stats.Stats;
import topk.TopK;

public class Main {

    public static void main(String[] args) throws Exception {

        String raw = "/user/auber/data_ple/clashroyale/gdc_battles.nljson";
        String gameData = "/user/smenadjlia/data/GameData";
        String games = "/user/smenadjlia/data/games";
        String stats = "/user/smenadjlia/data/stats";
        String statsData = "/user/smenadjlia/data/StatsData";
        String output = "/user/smenadjlia/data-test/res-all";
        String k = "10";
        String seed = "a-0";
        String stat = "0";

        // Filter.runJob(raw, output);
        Stats.runJob(games, output, seed);
        // TopK.runJob(stats, output, k, stat);
        
        // for (int m = 1; m < 12; m++) {
        // String monthSeed = "m-" + m;
        // String monthOutput = "/user/smenadjlia/data/deck-stats/" + monthSeed;
        // Stats.runJob(gameData, monthOutput, monthSeed);
        // }


        // String inputFile = args[0];
        // String outputFile = args[1];
        // String k = args[2];
        // String seed = args[3];
        // String stat = args[4];

        // Stats.runJob(inputFile, outputFile, seed);
        // Stats.runJob(inputFile, outputFile, k, stat);
    }
}