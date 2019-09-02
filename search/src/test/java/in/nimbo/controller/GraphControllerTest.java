package in.nimbo.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.dao.hbase.HBaseDAO;
import in.nimbo.config.SparkConfig;
import in.nimbo.entity.GraphResponse;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class GraphControllerTest {
    private static GraphController controller;
    private static HBaseDAO hBaseDAO;
    private static SparkConfig sparkConfig;

    @BeforeClass
    public static void init() {
        hBaseDAO = mock(HBaseDAO.class);
        HBaseConfig hBaseConfig = HBaseConfig.load();
        sparkConfig = SparkConfig.load();
        ObjectMapper mapper = new ObjectMapper();
        controller = new GraphController(hBaseDAO, hBaseConfig, sparkConfig, mapper);
    }

    @Test
    public void testWordGraph() throws IOException {
        GraphResponse wordGraph = controller.wordGraph();
        assertTrue(wordGraph.getEdges().size() == 2);
        assertTrue(wordGraph.getEdges().get(0).getFrom().equals("hello"));
        assertTrue(wordGraph.getEdges().get(1).getFrom().equals("hello"));
        assertTrue(wordGraph.getEdges().get(0).getTo().equals("hi"));
        assertTrue(wordGraph.getEdges().get(1).getTo().equals("bye"));
        assertTrue(wordGraph.getEdges().get(0).getWidth() == 10);
        assertTrue(wordGraph.getEdges().get(1).getWidth() == 1);
        assertTrue(wordGraph.getNodes().size() == 3);
        assertTrue(wordGraph.getNodes().get(0).getId().equals("hello"));
        assertTrue(wordGraph.getNodes().get(1).getId().equals("hi"));
        assertTrue(wordGraph.getNodes().get(2).getId().equals("bye"));
        assertTrue(Double.valueOf(sparkConfig.getWordNodeSize()).equals(wordGraph.getNodes().get(0).getFont().getSize()));
        assertTrue(Double.valueOf(sparkConfig.getWordNodeSize()).equals(wordGraph.getNodes().get(1).getFont().getSize()));
        assertTrue(Double.valueOf(sparkConfig.getWordNodeSize()).equals(wordGraph.getNodes().get(2).getFont().getSize()));
    }

    @Test
    public void testSiteGraphWithoutParameter() throws IOException {
        GraphResponse siteGraph = controller.siteGraph(null);
        assertTrue(siteGraph.getNodes().size() == 2);
        assertTrue(siteGraph.getNodes().get(0).getId().equals("stackoverflow.com"));
        assertTrue(siteGraph.getNodes().get(0).getFont().getSize() == sparkConfig.getMinNode());
        assertTrue(siteGraph.getNodes().get(1).getFont().getSize() == sparkConfig.getMaxNode());
        assertTrue(siteGraph.getNodes().get(1).getId().equals("google.com"));
        assertTrue(siteGraph.getEdges().size() == 2);
        assertTrue(siteGraph.getEdges().get(0).getFrom().equals("stackoverflow.com"));
        assertTrue(siteGraph.getEdges().get(1).getFrom().equals("google.com"));
        assertTrue(siteGraph.getEdges().get(0).getTo().equals("google.com"));
        assertTrue(siteGraph.getEdges().get(1).getTo().equals("stackoverflow.com"));
        assertTrue(siteGraph.getEdges().get(0).getWidth() == sparkConfig.getMaxEdge());
        assertTrue(siteGraph.getEdges().get(1).getWidth() == sparkConfig.getMinEdge());
    }
}
