package in.nimbo.entity;

public class Node {
    private String id;
    private double rank;
    private int numOfPages;

    public Node() {}

    public Node(String id, double rank) {
        this.id = id;
        this.rank = rank;
        numOfPages = 1;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getRank() {
        return rank;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }

    public int getNumOfPages() {
        return numOfPages;
    }

    public void setNumOfPages(int numOfPages) {
        this.numOfPages = numOfPages;
    }
}
