package in.nimbo.controller;

import in.nimbo.config.SparkConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.entity.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public class SearchController {
    private ElasticDAO elasticDAO;
    private SparkConfig config;

    public SearchController(ElasticDAO elasticDAO, SparkConfig config) {
        this.elasticDAO = elasticDAO;
        this.config = config;
    }

    public List<Page> search(String query, String site) {
        return elasticDAO.search(query, site);
    }

    public SiteGraphResponse siteGraph() throws FileNotFoundException {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("site-graph");
        Scanner scanner = new Scanner(stream);
        Set<Node> nodes = new HashSet<>();
        List<Edge> edges = new ArrayList<>();
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            int commaIndex = line.indexOf(",");
            String domain = line.substring(2, commaIndex);
            line = line.substring(commaIndex + 1);
            commaIndex = line.indexOf(",");
            String rankString = line.substring(0, commaIndex);
            double rank = Double.valueOf(rankString);
            line = line.substring(commaIndex + 1);
            int closeIndex = line.indexOf("]");
            String countString = line.substring(0, closeIndex);
            int count = Integer.valueOf(countString);
            Font font = new Font(rank);
            Node node = new Node(domain, font, count);
            nodes.add(node);
            line = line.substring(closeIndex + 3);
            commaIndex = line.indexOf(",");
            domain = line.substring(0, commaIndex);
            line = line.substring(commaIndex + 1);
            commaIndex = line.indexOf(",");
            rankString = line.substring(0, commaIndex);
            rank = Double.valueOf(rankString);
            line = line.substring(commaIndex + 1);
            closeIndex = line.indexOf("]");
            countString = line.substring(0, closeIndex);
            count = Integer.valueOf(countString);
            font = new Font(rank);
            Node node2 = new Node(domain, font, count);
            nodes.add(node2);
            line = line.substring(closeIndex + 1);
            closeIndex = line.indexOf("]");
            String weightString = line.substring(1, closeIndex);
            int weight = Integer.valueOf(weightString);
            Edge edge = new Edge(node.getDomain(), node2.getDomain(), weight);
            edges.add(edge);
        }
        List<Node> nodeList = new ArrayList<>(nodes);
        List<Node> filteredNodes = nodeList.stream().filter(node -> node.getFont().getSize() > config.getFilterNode()).collect(Collectors.toList());
        OptionalDouble minNode = filteredNodes.stream().mapToDouble(node -> node.getFont().getSize()).min();
        OptionalDouble maxNode = filteredNodes.stream().mapToDouble(node -> node.getFont().getSize()).max();
        filteredNodes.forEach(node -> node.getFont().setSize((node.getFont().getSize() - minNode.getAsDouble()) / (maxNode.getAsDouble() - minNode.getAsDouble()) * (config.getMaxNode() - config.getMinNode()) + config.getMinNode()));
        double min = getMin(filteredNodes);
        normalize(filteredNodes, min);
        List<Edge> filteredEdges = edges.stream().filter(edge -> edge.getWeight() > config.getFilterEdge()).collect(Collectors.toList());
        OptionalInt minEdge = filteredEdges.stream().mapToInt(Edge::getWeight).min();
        OptionalInt maxEdge = filteredEdges.stream().mapToInt(Edge::getWeight).max();
        filteredEdges.forEach(edge -> edge.setWeight((int)((double)edge.getWeight() - minEdge.getAsInt()) / (maxEdge.getAsInt() - minEdge.getAsInt()) * (config.getMaxEdge() - config.getMinEdge()) + config.getMinEdge()));
        return new SiteGraphResponse(filteredNodes, filteredEdges);
    }

    private void normalize(List<Node> nodeList, double min) {
        for (Node node : nodeList) {
            node.getFont().setSize(node.getFont().getSize() / min);
        }
    }

    private double getMin(List<Node> nodes) {
        double min = Double.MAX_VALUE;
        for (Node node : nodes) {
            if (min > node.getFont().getSize()) {
                min = node.getFont().getSize();
            }
        }
        return min;
    }
}
