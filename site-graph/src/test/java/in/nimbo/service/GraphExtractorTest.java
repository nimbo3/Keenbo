package in.nimbo.service;

import in.nimbo.App;
import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.config.HBaseSiteConfig;
import in.nimbo.common.entity.GraphResult;
import in.nimbo.common.utility.SparkUtility;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraphExtractorTest {
    private static SparkSession sparkSession;
    private static JavaSparkContext javaSparkContext;


    @BeforeClass
    public static void init() {
        LogManager.getLogger("org").setLevel(Level.WARN);
        sparkSession = App.loadSpark("test", true);
        javaSparkContext = SparkUtility.getJavaSparkContext(sparkSession);
    }

    @AfterClass
    public static void finish() {
        sparkSession.stop();
    }

    @Test
    public void graphExtractor() {
        HBaseSiteConfig hBaseSiteConfig = HBaseSiteConfig.load();
        List<Result> resultList = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            List<Cell> cellList = new ArrayList<>();
            cellList.add(CellUtil.createCell(Bytes.toBytes(String.valueOf(i)), hBaseSiteConfig.getInfoColumnFamily(),
                    hBaseSiteConfig.getRankColumn(),
                    new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes(String.valueOf(i)), Bytes.toBytes("1")));
            cellList.add(CellUtil.createCell(Bytes.toBytes(String.valueOf(i)), hBaseSiteConfig.getDomainColumnFamily(),
                    Bytes.toBytes(String.valueOf((i + 1) % 4)),
                    new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes(String.valueOf(i)), Bytes.toBytes("1")));
            resultList.add(Result.create(cellList));
        }
        JavaRDD<Result> hBaseRDD = javaSparkContext.parallelize(resultList);
        GraphResult graphResult = GraphExtractor.extract(hBaseSiteConfig, sparkSession, hBaseRDD);
        List<Row> nodes = graphResult.getNodes().collectAsList();
        List<Row> edges = graphResult.getEdges().collectAsList();
        assertEquals("[3,[3.0]]", nodes.get(0).toString());
        assertEquals("[2,[2.0]]", nodes.get(1).toString());
        assertEquals("[1,[1.0]]", nodes.get(2).toString());
        assertEquals("[3,0,3]", edges.get(0).toString());
        assertEquals("[2,3,2]", edges.get(1).toString());
        assertEquals("[1,2,1]", edges.get(2).toString());
    }

    @Test
    public void siteExtractor() {
        HBasePageConfig hBasePageConfig = HBasePageConfig.load();
        HBaseSiteConfig hBaseSiteConfig = HBaseSiteConfig.load();
        List<Result> resultList = new ArrayList<>();
        List<String> rows = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            String row = "http://com.a" + i;
            String anchor = "http://b.com/" + i;
            List<Cell> cellList = new ArrayList<>();
            cellList.add(CellUtil.createCell(Bytes.toBytes(row), hBasePageConfig.getDataColumnFamily(), hBasePageConfig.getRankColumn(),
                    new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes(String.valueOf(i)), Bytes.toBytes("1")));
            cellList.add(CellUtil.createCell(Bytes.toBytes(row), hBasePageConfig.getAnchorColumnFamily(),
                    Bytes.toBytes(anchor),
                    new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes(String.valueOf(i)), Bytes.toBytes("1")));
            resultList.add(Result.create(cellList));
            rows.add("a"+ i + ".com");
        }
        List<Cell> cellList = new ArrayList<>();
        cellList.add(CellUtil.createCell(Bytes.toBytes("https://com.b"), hBasePageConfig.getDataColumnFamily(), hBasePageConfig.getRankColumn(),
                new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes(String.valueOf(1)), Bytes.toBytes("1")));
        resultList.add(Result.create(cellList));
        rows.add("b.com");

        JavaRDD<Result> hBaseRDD = javaSparkContext.parallelize(resultList);
        Tuple2<JavaPairRDD<ImmutableBytesWritable, Put>, JavaPairRDD<ImmutableBytesWritable, Put>> result =
                SiteExtractor.extract(hBasePageConfig, hBaseSiteConfig, sparkSession, hBaseRDD);
        Collection<Put> nodes = result._1.collectAsMap().values();
        Collection<Put> edges = result._2.collectAsMap().values();
        for (Put node : nodes) {
            assertTrue(rows.contains(Bytes.toString(node.getRow())));
        }
        assertEquals(1, edges.size());
        for (Put edge : edges) {
            assertTrue(rows.contains(Bytes.toString(edge.getRow())));
        }
    }
}
