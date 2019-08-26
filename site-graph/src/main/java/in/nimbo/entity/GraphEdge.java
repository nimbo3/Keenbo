package in.nimbo.entity;

public class GraphEdge {
    private String from;
    private String to;
    private long weight;

    public GraphEdge() {}

    public GraphEdge(String from, String to, long weight) {
        this.from = from;
        this.to = to;
        this.weight = weight;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public long getWeight() {
        return weight;
    }

    public void setWeight(long weight) {
        this.weight = weight;
    }
}
