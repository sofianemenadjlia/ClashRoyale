package deck;

import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class DeckStats implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private String id;
        private int wins;
        private double ratio;
        private int uses;
        @JsonIgnore // This annotation will exclude the players field from serialization
        private Set<String> players;
        private int nbPlayers;
        private int clanLevel;
        private double averageLevel;

        public DeckStats(Deck deck, String combinationKey) {
            players = new HashSet<>();
            this.id = combinationKey;

            this.wins = deck.getWins();
            this.ratio = deck.getRatio();
            this.uses = deck.getUses();
            for (String player: deck.getPlayers())
                this.addPlayer(player);
            this.nbPlayers = this.players.size();
            this.clanLevel = deck.getClanLevel();
            this.averageLevel = deck.getAverageLevel();
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

        public DeckStats combine(DeckStats other) {
            
            this.setWins(this.getWins() + other.getWins());
            
            this.setUses(this.getUses() + other.getUses());
                        
            Set<String> players = new HashSet<>(this.getPlayers());

            players.addAll(other.getPlayers());
            this.setPlayers(players);
            
            this.setClanLevel(Math.max(this.getClanLevel(), other.getClanLevel()));
            
            this.setAverageLevel(this.getAverageLevel() + other.getAverageLevel());
            
            return this;
        }
    }