package de.xiekang.comparison;

public class Player implements Comparable<Player> {
    private int ranking;
    private String name;
    private int age;

    public Player() {
    }

    public Player(int ranking, String name, int age) {
        this.ranking = ranking;
        this.name = name;
        this.age = age;
    }

    public int getRanking() {
        return ranking;
    }

    public void setRanking(int ranking) {
        this.ranking = ranking;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String toString() {
        return String.format("Player %s with %s ages at %s place", getName(), getAge(), getRanking());
    }

    @Override
    public int compareTo(Player o) {
        return Integer.compare(getRanking(), o.getRanking());
    }
}