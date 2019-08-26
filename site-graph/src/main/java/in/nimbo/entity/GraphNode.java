package in.nimbo.entity;

import java.util.Objects;

public class GraphNode {
    private String id;
    private double size;

    public GraphNode() {}

    public GraphNode(String id, double rank) {
        this.id = id;
        this.size = rank;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getRank() {
        return size;
    }

    public void setRank(double rank) {
        this.size = rank;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphNode node = (GraphNode) o;
        return Objects.equals(id, node.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
