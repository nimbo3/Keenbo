package in.nimbo.entity;

import java.io.Serializable;

public class Page implements Serializable {
    private String id;
    private double rank;

    public Page() {
    }

    public Page(String id, double rank) {
        this.id = id;
        this.rank = rank;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }

    public String getId() {
        return id;
    }

    public double getRank() {
        return rank;
    }

    @Override
    public String toString() {
        return "Page{" +
                "id='" + id + '\'' +
                ", rank=" + rank +
                '}';
    }
}
