package deck;

import java.util.*;
import java.util.Set;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class Deck implements WritableComparable<Deck> {

    private String id;
    private int wins;
    private double ratio;
    private int uses;
    @JsonIgnore // This annotation will exclude the players field from serialization
    private Set<String> players;
    private int nbPlayers;
    private int clanLevel;
    private double averageLevel;

    public Deck() {
        // Initialization code, if necessary
    }

    // Default constructor
    public Deck(String id) {
        players = new HashSet<>();
        this.id = id;
    }

    public Deck(String id, int wins, double ratio, int uses, String player, int clanLevel, double averageLevel) {

        players = new HashSet<>();
        this.id = id;
        this.wins = wins;
        this.ratio = ratio;
        this.uses = uses;
        this.addPlayer(player);
        this.nbPlayers = this.players.size();
        this.clanLevel = clanLevel;
        this.averageLevel = averageLevel;
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

    public double getRatio() {
        return ratio;
    }

    public void setRatio(double ratio) {
        this.ratio = ratio;
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

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeInt(wins);
        out.writeDouble(ratio);
        out.writeInt(uses);
        out.writeInt(players.size());
        for (String player : players) {
            out.writeUTF(player);
        }
        out.writeInt(nbPlayers);
        out.writeInt(clanLevel);
        out.writeDouble(averageLevel);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        wins = in.readInt();
        ratio = in.readDouble();
        uses = in.readInt();
        int playersSize = in.readInt();
        players = new HashSet<>();
        for (int i = 0; i < playersSize; i++) {
            players.add(in.readUTF());
        }
        nbPlayers = in.readInt();
        clanLevel = in.readInt();
        averageLevel = in.readDouble();
    }

    @Override
    public int compareTo(Deck other) {

        int result = Integer.compare(wins, other.wins);
        if (result != 0)
            return result;
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
                ratio == deck.ratio &&
                uses == deck.uses &&
                nbPlayers == deck.nbPlayers &&
                clanLevel == deck.clanLevel &&
                averageLevel == deck.averageLevel;
    }

    @Override
    public int hashCode() {
        return Objects.hash(wins, ratio, uses, nbPlayers, clanLevel, averageLevel);
    }

    @Override
    public String toString() {
        return "Deck{" +
                "id=" + id +
                ", wins=" + wins +
                ", ratio=" + ratio +
                ", uses=" + uses +
                ", nbPlayers=" + nbPlayers +
                ", clanLevel=" + clanLevel +
                ", averageLevel=" + averageLevel +
                '}';
    }

    public String toJson() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null; // or handle the exception as per your requirement
        }
    }
}
