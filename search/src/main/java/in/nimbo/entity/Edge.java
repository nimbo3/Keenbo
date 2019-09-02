package in.nimbo.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Edge {
    private String from;
    private String to;
    private int width;

    public Edge(){}

    public Edge(String from, String to, int width) {
        this.from = from;
        this.to = to;
        this.width = width;
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

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }
}
