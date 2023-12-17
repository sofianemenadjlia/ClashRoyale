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
        String statsData = "/user/smenadjlia/data/StatsData";
        String output = "/user/smenadjlia/data-test/res-all";
        String k = "10";
        String seed = "m-7";
        String stat = "4";

        // Filter.runJob(raw, output);
        // Stats.runJob(gameData, output);
        TopK.runJob(statsData, output, k, stat);

    }
}