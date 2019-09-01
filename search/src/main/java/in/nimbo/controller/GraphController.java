package in.nimbo.controller;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.utility.FileUtility;
import in.nimbo.config.SparkConfig;
import in.nimbo.common.dao.hbase.HBaseDAO;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Font;
import in.nimbo.entity.GraphResponse;
import in.nimbo.entity.Node;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.stream.Collectors;

public class GraphController {
    private GraphResponse wordGraph;
    private GraphResponse siteGraph;
    private HBaseDAO hBaseDAO;
    private HBaseConfig hBaseConfig;
    private SparkConfig config;
    private Gson gson;

    public GraphController(HBaseDAO hBaseDAO, HBaseConfig hBaseConfig, SparkConfig config, Gson gson) {
        this.hBaseDAO = hBaseDAO;
        this.hBaseConfig = hBaseConfig;
        this.config = config;
        this.gson = gson;
    }

    private List<Edge> getEdges(Result result, String link) {
        return result.listCells().stream().filter(cell -> CellUtil.matchingFamily(cell, hBaseConfig.getDomainColumnFamily()))
                .map(cell -> new Edge(link, Bytes.toString(CellUtil.cloneQualifier(cell)), Integer.valueOf(Bytes.toString(CellUtil.cloneValue(cell)))))
                .collect(Collectors.toList());
    }

    private Node getNode(Result result) {
        String row = Bytes.toString(result.getRow());
        double rank = Double.valueOf(Bytes.toString(result.getValue(hBaseConfig.getInfoColumnFamily()
                , hBaseConfig.getSiteRankColumn())));
        int pageCount = Integer.valueOf(Bytes.toString(result.getValue(hBaseConfig.getInfoColumnFamily()
                , hBaseConfig.getCountColumn())));
        Font font = new Font(rank);
        return new Node(row, font, pageCount);
    }

    public GraphResponse siteGraph(String link) throws IOException {
        if (link != null) {
            Result result = hBaseDAO.get(link);
            List<Edge> edges = getEdges(result, link);
            Node node = getNode(result);
            List<String> tos = edges.stream().map(Edge::getTo).collect(Collectors.toList());
            Result[] results = hBaseDAO.getBulk(tos);
            List<Node> nodes = new ArrayList<>();
            nodes.add(node);
            for (Result toResult : results) {
                nodes.add(getNode(toResult));
            }
            return getResponse(nodes, edges);
        }
        if (siteGraph != null) {
            return siteGraph;
        }
        String siteGraphNodesJson = FileUtility.readFileFromResource("site-graph-vertices");
        List<Node> nodeList = gson.fromJson(siteGraphNodesJson, new TypeToken<List<Node>>(){}.getType());
        String siteGraphEdgesJson = FileUtility.readFileFromResource("site-graph-edges");
        List<Edge> edges = gson.fromJson(siteGraphEdgesJson, new TypeToken<List<Edge>>(){}.getType());
        return siteGraph = getResponse(nodeList, edges);
    }

    private GraphResponse getResponse(List<Node> nodeList, List<Edge> edges) {
        List<Node> filteredNodes = nodeList.stream().filter(node -> node.getFont().getSize() > config.getFilterNode()).collect(Collectors.toList());
        DoubleSummaryStatistics nodesSummary = filteredNodes.stream().mapToDouble(node -> node.getFont().getSize()).summaryStatistics();
        double maxNode = nodesSummary.getMax();
        double minNode = nodesSummary.getMin();
        filteredNodes.forEach(node -> node.getFont().setSize((node.getFont().getSize() - minNode) / (maxNode - minNode) * (config.getMaxNode() - config.getMinNode()) + config.getMinNode()));
        List<Edge> filteredEdges = edges.stream().filter(edge -> edge.getWidth() > config.getFilterEdge()).collect(Collectors.toList());
        filteredEdges = filteredEdges.stream().filter(edge -> filteredNodes.stream().anyMatch(dst -> dst.getId().equals(edge.getTo()) &&
                filteredNodes.stream().anyMatch(src -> src.getId().equals(edge.getFrom()) && (!dst.getId().equals(src.getId()))))).collect(Collectors.toList());
        IntSummaryStatistics edgesSummary = filteredEdges.stream().mapToInt(Edge::getWidth).summaryStatistics();
        int maxEdge = edgesSummary.getMax();
        int minEdge = edgesSummary.getMin();
        filteredEdges.forEach(edge -> scaleEdge(edge, minEdge, maxEdge));
        return new GraphResponse(filteredNodes, filteredEdges);
    }

    public GraphResponse wordGraph() {
        if (wordGraph != null) {
            return wordGraph;
        }
        String wordGraphNodesJson = FileUtility.readFileFromResource("word-graph-vertices");
        List<Node> nodeList = new Gson().fromJson(wordGraphNodesJson, new TypeToken<List<Node>>(){}.getType());
        String wordGraphEdgesJson = FileUtility.readFileFromResource("word-graph-edges");
        List<Edge> edges = new Gson().fromJson(wordGraphEdgesJson, new TypeToken<List<Edge>>(){}.getType());

        nodeList.forEach(node -> node.getFont().setSize(config.getWordNodeSize()));
        List<Edge> filteredEdges = edges.stream().filter(edge -> edge.getWidth() > config.getWordFilterEdge()).collect(Collectors.toList());
        List<Edge> filteredBadEdges = filteredEdges.stream().filter(edge -> nodeList.stream().anyMatch(dst -> dst.getId().equals(edge.getTo()) &&
                nodeList.stream().anyMatch(src -> src.getId().equals(edge.getFrom()) && (!dst.getId().equals(src.getId()))))).collect(Collectors.toList());
        List<Node> filteredBadNodes = nodeList.stream().filter(node -> filteredBadEdges.stream().anyMatch(edge -> edge.getFrom().equals(node.getId()) || edge.getTo().equals(node.getId()))).collect(Collectors.toList());
        IntSummaryStatistics edgesSummary = filteredEdges.stream().mapToInt(Edge::getWidth).summaryStatistics();
        int maxEdge = edgesSummary.getMax();
        int minEdge = edgesSummary.getMin();
        filteredEdges.forEach(edge -> scaleEdge(edge, minEdge, maxEdge));
        return wordGraph = new GraphResponse(filteredBadNodes, filteredBadEdges);
    }

    private void scaleEdge(Edge edge, int minEdge, int maxEdge) {
        int weight = ((int) (((double) edge.getWidth() - minEdge) * (config.getMaxEdge() - config.getMinEdge()) / (maxEdge - minEdge) + config.getMinEdge()));
        edge.setWidth(weight);
    }
}
