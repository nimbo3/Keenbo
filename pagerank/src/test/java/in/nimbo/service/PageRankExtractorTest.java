package in.nimbo.service;

import in.nimbo.App;
import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.utility.SparkUtility;
import in.nimbo.config.PageRankConfig;
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

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
        HBaseConfig hBasePageConfig = HBaseConfig.load();
        PageRankConfig pageRankConfig = PageRankConfig.load();
        pageRankConfig.setMaxIter(1);
        List<Result> resultList = new ArrayList<>();
        List<String> rows = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            String row = "http://com.a" + i;
            String anchor = "https://b.com";
            List<Cell> cellList = new ArrayList<>();
            cellList.add(CellUtil.createCell(Bytes.toBytes(row), hBasePageConfig.getDataColumnFamily(), hBasePageConfig.getPageRankColumn(),
                    new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes(String.valueOf(i)), Bytes.toBytes("1")));
            cellList.add(CellUtil.createCell(Bytes.toBytes(row), hBasePageConfig.getAnchorColumnFamily(),
                    Bytes.toBytes(anchor),
                    new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes(String.valueOf(i)), Bytes.toBytes("1")));
            resultList.add(Result.create(cellList));
            rows.add("a"+ i + ".com");
        }
        List<Cell> cellList = new ArrayList<>();
        cellList.add(CellUtil.createCell(Bytes.toBytes("https://com.b"), hBasePageConfig.getDataColumnFamily(), hBasePageConfig.getPageRankColumn(),
                new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes(String.valueOf(1)), Bytes.toBytes("1")));
        resultList.add(Result.create(cellList));
        rows.add("https://com.b");

        JavaRDD<Result> hBaseRDD = javaSparkContext.parallelize(resultList);
        JavaPairRDD<ImmutableBytesWritable, Put> result =
                PageRankExtractor.extract(hBasePageConfig, pageRankConfig, sparkSession, hBaseRDD);
        assertEquals(4, result.count());
    }
}
