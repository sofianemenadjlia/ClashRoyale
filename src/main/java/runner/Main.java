package runner;

import num_plays.NumPlays;
import clash_royale.ClashRoyale;
import unique_players.UniquePlayers;
import average_difference.AverageDifference;
import clan_level.ClanLevel;
import average.Average;

public class Main {

    public static void main(String[] args) throws Exception {

        String arg1 = "/user/auber/data_ple/clashroyale/gdc_battles.nljson";
        String arg2 = "/user/smenadjlia/data-test/res-all";
        String arg3 = "10";
        String arg4 = "m-7";

        Average.runJob(arg1, arg2, arg3, arg4);

        System.out.println("hi");
    }
}