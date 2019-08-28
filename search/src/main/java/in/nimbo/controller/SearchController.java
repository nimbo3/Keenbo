package in.nimbo.controller;

import in.nimbo.config.SparkConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.entity.*;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SearchController {
    private static final String SEARCH_QUERY_REGEX = "^(.*?)(\\s+site:(.*))?$";
    private ElasticDAO elasticDAO;
    private SparkConfig config;

    public SearchController(ElasticDAO elasticDAO, SparkConfig config) {
        this.elasticDAO = elasticDAO;
        this.config = config;
    }

    public List<Page> search(String query) {
        Pattern pattern = Pattern.compile(SEARCH_QUERY_REGEX);
        Matcher matcher = pattern.matcher(query);
        if (matcher.find()) {
            String extractedQuery = matcher.group(1);
            String site = matcher.group(3);
            if (site == null)
                site = "";
            return elasticDAO.search(extractedQuery, site);
        }
        throw new AssertionError();
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
        DoubleSummaryStatistics nodesSummary = filteredNodes.stream().mapToDouble(node -> node.getFont().getSize()).summaryStatistics();
        double maxNode = nodesSummary.getMax();
        double minNode = nodesSummary.getMin();
        filteredNodes.forEach(node -> node.getFont().setSize((node.getFont().getSize() - minNode) / (maxNode - minNode) * (config.getMaxNode() - config.getMinNode()) + config.getMinNode()));
        List<Edge> filteredEdges = edges.stream().filter(edge -> edge.getWidth() > config.getFilterEdge()).collect(Collectors.toList());
        filteredEdges = filteredEdges.stream().filter(edge -> filteredNodes.stream().anyMatch(dst -> dst.getDomain().equals(edge.getDst()) &&
                filteredNodes.stream().anyMatch(src -> src.getDomain().equals(edge.getSrc()) && (!dst.getDomain().equals(src.getDomain()))))).collect(Collectors.toList());
        IntSummaryStatistics edgesSummary = filteredEdges.stream().mapToInt(Edge::getWidth).summaryStatistics();
        int maxEdge = edgesSummary.getMax();
        int minEdge = edgesSummary.getMin();
        filteredEdges.forEach(edge -> scaleEdge(edge, minEdge, maxEdge));
        return new SiteGraphResponse(filteredNodes, filteredEdges);
    }

    private void scaleEdge(Edge edge, int minEdge, int maxEdge) {
        int weight = ((int) (((double) edge.getWidth() - minEdge) * (config.getMaxEdge() - config.getMinEdge()) / (maxEdge - minEdge) + config.getMinEdge()));
        edge.setWidth(weight);
    }
}
