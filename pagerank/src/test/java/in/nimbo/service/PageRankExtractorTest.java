package in.nimbo.service;

import in.nimbo.App;
import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.utility.SparkUtility;
import in.nimbo.config.PageRankConfig;
import in.nimbo.entity.Page;
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
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PageRankExtractorTest {
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
    public void siteExtractor() {
        HBasePageConfig hBasePageConfig = HBasePageConfig.load();
        PageRankConfig pageRankConfig = PageRankConfig.load();
        pageRankConfig.setMaxIter(1);
        List<Result> resultList = new ArrayList<>();
        List<String> rows = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            String row = "http://com.a" + i;
            String anchor = "https://b.com";
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
        rows.add("https://com.b");

        JavaRDD<Result> hBaseRDD = javaSparkContext.parallelize(resultList);
        Tuple2<JavaPairRDD<ImmutableBytesWritable, Put>, JavaRDD<Page>> result =
                PageRankExtractor.extract(hBasePageConfig, pageRankConfig, sparkSession, hBaseRDD);
        Collection<Put> pageRankHBase = result._1.repartition(1).collectAsMap().values();
        List<Page> pageRankElastic = result._2.collect();
        System.out.println(pageRankHBase);
        System.out.println(pageRankElastic);
        for (Put node : pageRankHBase) {
            assertTrue(rows.contains(Bytes.toString(node.getRow())));
        }
        assertEquals(1, pageRankHBase.size());
        assertEquals(4, pageRankElastic.size());
    }
}
