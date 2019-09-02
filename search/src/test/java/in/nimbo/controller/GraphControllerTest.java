package in.nimbo.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.dao.hbase.HBaseDAO;
import in.nimbo.common.dao.hbase.HBaseDAOImpl;
import in.nimbo.config.SparkConfig;
import in.nimbo.entity.GraphResponse;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GraphControllerTest {
    private static GraphController controller;
    private static HBaseDAO hBaseDAO;
    private static SparkConfig sparkConfig;

    @BeforeClass
    public static void init() throws IOException {
        Connection connection = ConnectionFactory.createConnection();
        HBaseConfig hBaseConfig = HBaseConfig.load();
        hBaseDAO = new HBaseDAOImpl(connection, hBaseConfig);
        sparkConfig = SparkConfig.load();
        ObjectMapper mapper = new ObjectMapper();
        controller = new GraphController(hBaseDAO, hBaseConfig, sparkConfig, mapper);
        System.out.println("creating table");
        Admin admin = connection.getAdmin();
        HColumnDescriptor[] columnDescriptors = new HColumnDescriptor[2];
        columnDescriptors[0] = new HColumnDescriptor(hBaseConfig.getDomainColumnFamily());
        columnDescriptors[1] = new HColumnDescriptor(hBaseConfig.getInfoColumnFamily());
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(hBaseConfig.getSiteTable()));
        descriptor.addFamily(columnDescriptors[0]).addFamily(columnDescriptors[1]);
        admin.createTable(descriptor);
        System.out.println("table created");
        Table table = connection.getTable(TableName.valueOf(hBaseConfig.getSiteTable()));
        List<Put> puts = new ArrayList<>();
        Put stackoverflowPut = new Put(Bytes.toBytes("stackoverflow.com"));
        stackoverflowPut.addColumn(hBaseConfig.getInfoColumnFamily(), hBaseConfig.getSiteRankColumn(), Bytes.toBytes("2"));
        stackoverflowPut.addColumn(hBaseConfig.getInfoColumnFamily(), hBaseConfig.getCountColumn(), Bytes.toBytes("1"));
        stackoverflowPut.addColumn(hBaseConfig.getDomainColumnFamily(), Bytes.toBytes("google.com"), Bytes.toBytes("4"));
        stackoverflowPut.addColumn(hBaseConfig.getDomainColumnFamily(), Bytes.toBytes("facebook.com"), Bytes.toBytes("4"));
        puts.add(stackoverflowPut);
        Put googlePut = new Put(Bytes.toBytes("google.com"));
        googlePut.addColumn(hBaseConfig.getInfoColumnFamily(), hBaseConfig.getSiteRankColumn(), Bytes.toBytes("8"));
        googlePut.addColumn(hBaseConfig.getInfoColumnFamily(), hBaseConfig.getCountColumn(), Bytes.toBytes("1"));
        puts.add(googlePut);
        Put facebookPut = new Put(Bytes.toBytes("facebook.com"));
        facebookPut.addColumn(hBaseConfig.getInfoColumnFamily(), hBaseConfig.getSiteRankColumn(), Bytes.toBytes("4"));
        facebookPut.addColumn(hBaseConfig.getInfoColumnFamily(), hBaseConfig.getCountColumn(), Bytes.toBytes("1"));
        puts.add(facebookPut);
        table.put(puts);
        System.out.println("data added to table");
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

    @Test
    public void testSiteGraphWithParameter() throws IOException {
        GraphResponse siteGraph = controller.siteGraph("stackoverflow.com");
        assertTrue(siteGraph.getNodes().size() == 3);
        assertTrue(siteGraph.getNodes().get(0).getId().equals("stackoverflow.com"));
        assertTrue(siteGraph.getNodes().get(1).getId().equals("facebook.com"));
        assertTrue(siteGraph.getNodes().get(2).getId().equals("google.com"));
    }
}
