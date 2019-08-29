package in.nimbo.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.nimbo.config.SparkConfig;
import in.nimbo.dao.redis.LabelDAO;
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
    private ObjectMapper mapper;
    private LabelDAO labelDAO;

    public SearchController(ElasticDAO elasticDAO, SparkConfig config, ObjectMapper mapper, LabelDAO labelDAO) {
        this.elasticDAO = elasticDAO;
        this.config = config;
        this.mapper = mapper;
        this.labelDAO = labelDAO;
    }

    public List<Page> search(String query) {
        Pattern pattern = Pattern.compile(SEARCH_QUERY_REGEX);
        Matcher matcher = pattern.matcher(query);
        if (matcher.find()) {
            String extractedQuery = matcher.group(1);
            String site = matcher.group(3);
            if (site == null) {
                site = "";
            }
            List<Page> pages = elasticDAO.search(extractedQuery, site);
            pages.forEach(page -> labelDAO.add(page.getLink(), page.getLabel()));
            return pages;
        }
        throw new AssertionError();
    }

    public SiteGraphResponse siteGraph() throws FileNotFoundException, JsonProcessingException {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("site-graph");
        Scanner scanner = new Scanner(stream);
        StringBuilder jsonBuilder = new StringBuilder("");
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            jsonBuilder.append(line);
        }
        String json = jsonBuilder.toString();
        SiteGraphResponse siteGraphResponse = mapper.readValue(json, SiteGraphResponse.class);
        List<Node> nodeList = siteGraphResponse.getNodes();
        List<Node> filteredNodes = nodeList.stream().filter(node -> node.getFont().getSize() > config.getFilterNode()).collect(Collectors.toList());
        List<Edge> edges = siteGraphResponse.getEdges();
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
