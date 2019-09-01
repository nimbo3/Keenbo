package in.nimbo;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.entity.GraphResult;
import in.nimbo.common.utility.SparkUtility;
import in.nimbo.config.WordGraphConfig;
import in.nimbo.service.WordGraphExtractorService;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class WordGraphExtractorServiceTest {
    private static SparkSession sparkSession;
    private static JavaSparkContext javaSparkContext;
    private static HBaseConfig hBaseConfig;

    @BeforeClass
    public static void init() {
        WordGraphConfig siteGraphConfig = WordGraphConfig.load();
        LogManager.getLogger("org").setLevel(Level.WARN);
        sparkSession = App.loadSpark(siteGraphConfig.getAppName() + " test", true);
        javaSparkContext = SparkUtility.getJavaSparkContext(sparkSession);
        hBaseConfig = HBaseConfig.load();
    }

    @AfterClass
    public static void finish() {
        sparkSession.stop();
    }

    @Test
    public void graphExtractor() {
        List<Result> resultList = new ArrayList<>();
        resultList.add(createResult("http://in.nimbo", new String[]{"nimbo", "sahab", "internship", "program"}
                , new String[]{"http://google.com", "http://stackoverflow.com"}));
        resultList.add(createResult("http://com.google", new String[]{"google", "search", "engine", "crawler"}
                , new String[]{"http://nimbo.in", "http://stackoverflow.com"}));
        resultList.add(createResult("http://com.stackoverflow", new String[]{"program", "ask", "question", "answer"}
                , new String[]{"http://google.com"}));
        JavaRDD<Result> hBaseRDD = javaSparkContext.parallelize(resultList);
        GraphResult graphResult = WordGraphExtractorService.extract(hBaseConfig, sparkSession, hBaseRDD);
        List<Row> returnedNodes = graphResult.getNodes().collectAsList();
        List<String> returnedNodesString = new ArrayList<>();
        for (Row row: returnedNodes) {
            returnedNodesString.add(row.getString(0));
        }
        List<String> nodes = Arrays.asList("nimbo", "sahab", "internship", "program", "google", "search", "engine"
                , "ask", "question", "answer", "crawler");
        returnedNodesString.sort(String::compareTo);
        nodes.sort(String::compareTo);
        Assert.assertEquals(nodes, returnedNodesString);
        List<Row> edges = graphResult.getEdges().collectAsList();
        long sumLinks = 0;
        for (Row edge : edges) {
            Assert.assertTrue(nodes.contains(edge.getString(0)));
            Assert.assertTrue(nodes.contains(edge.getString(1)));
            sumLinks += edge.getLong(2);
        }
        Assert.assertSame(115L, sumLinks);
    }

    private Result createResult(String rowKey, String[] keywords, String[] anchors) {
        List<Cell> cellList = new ArrayList<>();
        cellList.add(CellUtil.createCell(Bytes.toBytes(rowKey), hBaseConfig.getDataColumnFamily(),
                hBaseConfig.getRankColumn(),
                new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes("1"), Bytes.toBytes("1")));
        for (String anchor : anchors) {
            cellList.add(CellUtil.createCell(Bytes.toBytes(rowKey), hBaseConfig.getAnchorColumnFamily(),
                    Bytes.toBytes(anchor),
                    new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes(1), Bytes.toBytes("1")));
        }
        for (String keyword : keywords) {
            cellList.add(CellUtil.createCell(Bytes.toBytes(rowKey), hBaseConfig.getDataColumnFamily(),
                    Bytes.toBytes(keyword),
                    new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes(1), Bytes.toBytes("1")));
        }
        return Result.create(cellList);
    }
}
