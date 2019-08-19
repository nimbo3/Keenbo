package in.nimbo.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Edge {
    @JsonProperty("from")
    private String src;
    @JsonProperty("to")
    private String dst;
    @JsonProperty("width")
    private int weight;

    public Edge(String src, String dst, int weight) {
        this.src = src;
        this.dst = dst;
        this.weight = weight;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDst() {
        return dst;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Edge edge = (Edge) o;

        if (src != null ? !src.equals(edge.src) : edge.src != null) return false;
        return dst != null ? dst.equals(edge.dst) : edge.dst == null;
    }

    @Override
    public int hashCode() {
        int result = src != null ? src.hashCode() : 0;
        result = 31 * result + (dst != null ? dst.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Edge{" +
                "src='" + src + '\'' +
                ", dst='" + dst + '\'' +
                ", weight=" + weight +
                '}';
    }
}
