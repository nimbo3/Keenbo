package in.nimbo.common.entity;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class GraphResult {
    private Dataset<Row> nodes;
    private Dataset<Row> edges;

    public GraphResult() {
    }

    public GraphResult(Dataset<Row> nodes, Dataset<Row> edges) {
        this.nodes = nodes;
        this.edges = edges;
    }

    public Dataset<Row> getNodes() {
        return nodes;
    }

    public void setNodes(Dataset<Row> nodes) {
        this.nodes = nodes;
    }

    public Dataset<Row> getEdges() {
        return edges;
    }

    public void setEdges(Dataset<Row> edges) {
        this.edges = edges;
    }
}
