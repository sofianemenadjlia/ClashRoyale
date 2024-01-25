package deck;

import org.apache.hadoop.io.WritableComparable;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClashGame implements WritableComparable<ClashGame> {
    private Integer month;
    private Integer week;
    private String player;
    private String player2;
    private String cards;
    private String cards2;
    private Integer crown;
    private Integer crown2;
    private Integer clanTr;
    private Integer clanTr2;
    private double deck;
    private double deck2;

    // Default constructor (required for reflection)
    public ClashGame() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(month);
        out.writeInt(week);
        out.writeUTF(player);
        out.writeUTF(player2);
        out.writeUTF(cards);
        out.writeUTF(cards2);
        out.writeInt(crown);
        out.writeInt(crown2);
        out.writeInt(clanTr);
        out.writeInt(clanTr2);
        out.writeDouble(deck);
        out.writeDouble(deck2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        month = in.readInt();
        week = in.readInt();
        player = in.readUTF();
        player2 = in.readUTF();
        cards = in.readUTF();
        cards2 = in.readUTF();
        crown = in.readInt();
        crown2 = in.readInt();
        clanTr = in.readInt();
        clanTr2 = in.readInt();
        deck = in.readDouble();
        deck2 = in.readDouble();
    }

    public int compareTo(ClashGame other) {
        // Compare month
        int monthComparison = month.compareTo(other.month);
        if (monthComparison != 0) {
            return monthComparison;
        }

        // Compare week
        int weekComparison = week.compareTo(other.week);
        if (weekComparison != 0) {
            return weekComparison;
        }

        // Compare player
        int playerComparison = player.compareTo(other.player);
        if (playerComparison != 0) {
            return playerComparison;
        }

        // Compare player2
        int player2Comparison = player2.compareTo(other.player2);
        if (player2Comparison != 0) {
            return player2Comparison;
        }

        // Compare cards
        int cardsComparison = cards.compareTo(other.cards);
        if (cardsComparison != 0) {
            return cardsComparison;
        }

        // Compare cards2
        int cards2Comparison = cards2.compareTo(other.cards2);
        if (cards2Comparison != 0) {
            return cards2Comparison;
        }

        // Compare crown
        int crownComparison = Integer.compare(crown, other.crown);
        if (crownComparison != 0) {
            return crownComparison;
        }

        // Compare crown2
        int crown2Comparison = Integer.compare(crown2, other.crown2);
        if (crown2Comparison != 0) {
            return crown2Comparison;
        }

        // Compare clanTr
        int clanTrComparison = Integer.compare(clanTr, other.clanTr);
        if (clanTrComparison != 0) {
            return clanTrComparison;
        }

        // Compare clanTr2
        int clanTr2Comparison = Integer.compare(clanTr2, other.clanTr2);
        if (clanTr2Comparison != 0) {
            return clanTr2Comparison;
        }

        // Compare deck
        int deckComparison = Double.compare(deck, other.deck);
        if (deckComparison != 0) {
            return deckComparison;
        }

        // Compare deck2
        return Double.compare(deck2, other.deck2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ClashGame clashGame = (ClashGame) o;

        return crown == clashGame.crown &&
                crown2 == clashGame.crown2 &&
                clanTr == clashGame.clanTr &&
                clanTr2 == clashGame.clanTr2 &&
                Double.compare(clashGame.deck, deck) == 0 &&
                Double.compare(clashGame.deck2, deck2) == 0 &&
                Objects.equals(month, clashGame.month) &&
                Objects.equals(week, clashGame.week) &&
                Objects.equals(player, clashGame.player) &&
                Objects.equals(player2, clashGame.player2) &&
                Objects.equals(cards, clashGame.cards) &&
                Objects.equals(cards2, clashGame.cards2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(month, week, player, player2, cards, cards2, crown, crown2, clanTr, clanTr2, deck, deck2);
    }

    // Getters and setters for all fields
    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public String getPlayer() {
        return player;
    }

    public void setPlayer(String player) {
        this.player = player;
    }

    public String getPlayer2() {
        return player2;
    }

    public void setPlayer2(String player2) {
        this.player2 = player2;
    }

    public String getCards() {
        return cards;
    }

    public void setCards(String cards) {
        this.cards = cards;
    }

    public String getCards2() {
        return cards2;
    }

    public void setCards2(String cards2) {
        this.cards2 = cards2;
    }

    public int getCrown() {
        return crown;
    }

    public void setCrown(int crown) {
        this.crown = crown;
    }

    public int getCrown2() {
        return crown2;
    }

    public void setCrown2(int crown2) {
        this.crown2 = crown2;
    }

    public int getClanTr() {
        return clanTr;
    }

    public void setClanTr(int clanTr) {
        this.clanTr = clanTr;
    }

    public int getClanTr2() {
        return clanTr2;
    }

    public void setClanTr2(int clanTr2) {
        this.clanTr2 = clanTr2;
    }

    public double getDeck() {
        return deck;
    }

    public void setDeck(double deck) {
        this.deck = deck;
    }

    public double getDeck2() {
        return deck2;
    }

    public void setDeck2(double deck2) {
        this.deck2 = deck2;
    }

    @Override
    public String toString() {
        return "ClashGame{" +
                "month='" + month + '\'' +
                ", week='" + week + '\'' +
                ", player='" + player + '\'' +
                ", player2='" + player2 + '\'' +
                ", cards='" + cards + '\'' +
                ", cards2='" + cards2 + '\'' +
                ", crown=" + crown +
                ", crown2=" + crown2 +
                ", clanTr=" + clanTr +
                ", clanTr2=" + clanTr2 +
                ", deck=" + deck +
                ", deck2=" + deck2 +
                '}';
    }

}
