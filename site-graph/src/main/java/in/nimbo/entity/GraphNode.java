package in.nimbo.entity;

import java.util.Objects;

public class GraphNode {
    private String id;
    private GraphFont font;

    public GraphNode() {}

    public GraphNode(String id, GraphFont font) {
        this.id = id;
        this.font = font;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public GraphFont getFont() {
        return font;
    }

    public void setFont(GraphFont font) {
        this.font = font;
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
