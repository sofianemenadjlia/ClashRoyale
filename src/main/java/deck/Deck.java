package deck;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class Deck implements WritableComparable<Deck> {

    private String id;
    private int wins;
    private int uses;
    private Set<String> players;
    private int nbPlayers;
    private int clanLevel;
    private double averageLevel;
    private int averageUses;

    public Deck() {
        // Initialization code, if necessary
    }

    // Default constructor
    public Deck(String id) {
        players = new HashSet<>();
        this.id = id;
    }

    public Deck(String id, int wins, int uses, String player, int clanLevel, double averageLevel,
            int averageUses) {

        players = new HashSet<>();
        this.id = id;
        this.wins = wins;
        this.uses = uses;
        this.addPlayer(player);
        this.nbPlayers = this.players.size();
        this.clanLevel = clanLevel;
        this.averageLevel = averageLevel;
        this.averageUses = averageUses;
    }

    // Getters and setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getWins() {
        return wins;
    }

    public void setWins(int wins) {
        this.wins = wins;
    }

    public int getUses() {
        return uses;
    }

    public void setUses(int uses) {
        this.uses = uses;
    }

    public Set<String> getPlayers() {
        return players;
    }

    public void setPlayers(Set<String> players) {
        this.players = players;
    }

    public void addPlayer(String player) {
        this.players.add(player);
        this.nbPlayers = players.size();
    }

    public int getNbPlayers() {
        return this.nbPlayers;
    }

    public int getClanLevel() {
        return clanLevel;
    }

    public void setClanLevel(int clanLevel) {
        this.clanLevel = clanLevel;
    }

    public double getAverageLevel() {
        return averageLevel;
    }

    public void setAverageLevel(double averageLevel) {
        this.averageLevel = averageLevel;
    }

    public int getAverageUses() {
        return averageUses;
    }

    public void setAverageUses(int averageUses) {
        this.averageUses = averageUses;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeInt(wins);
        out.writeInt(uses);
        out.writeInt(players.size());
        for (String player : players) {
            out.writeUTF(player);
        }
        out.writeInt(nbPlayers);
        out.writeInt(clanLevel);
        out.writeDouble(averageLevel);
        out.writeInt(averageUses);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        wins = in.readInt();
        uses = in.readInt();
        int playersSize = in.readInt();
        players = new HashSet<>();
        for (int i = 0; i < playersSize; i++) {
            players.add(in.readUTF());
        }
        nbPlayers = in.readInt();
        clanLevel = in.readInt();
        averageLevel = in.readDouble();
        averageUses = in.readInt();
    }

    @Override
    public int compareTo(Deck other) {
        // Implement this based on your sorting criteria
        // Example: sorting primarily by wins
        int result = Integer.compare(wins, other.wins);
        if (result != 0)
            return result;

        // Add further comparison logic if necessary
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Deck deck = (Deck) o;
        return wins == deck.wins &&
                uses == deck.uses &&
                players == deck.players &&
                clanLevel == deck.clanLevel &&
                Double.compare(deck.averageLevel, averageLevel) == 0 &&
                Integer.compare(deck.averageUses, averageUses) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(wins, uses, players, clanLevel, averageLevel, averageUses);
    }

    @Override
    public String toString() {
        return "Deck{" +
                "id=" + id +
                ", wins=" + wins +
                ", uses=" + uses +
                ", nbPlayers=" + nbPlayers +
                ", clanLevel=" + clanLevel +
                ", averageLevel=" + averageLevel +
                ", averageUses=" + averageUses +
                '}';
    }
}
