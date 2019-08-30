package in.nimbo.service;

import in.nimbo.App;
import in.nimbo.common.config.HBaseSiteConfig;
import in.nimbo.common.entity.GraphResult;
import in.nimbo.common.utility.SparkUtility;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraphExtractorTest {
    private static SparkSession sparkSession;
    private static JavaSparkContext javaSparkContext;

    @BeforeClass
    public static void init() {
        sparkSession = App.loadSpark("test", true);
        javaSparkContext = SparkUtility.getJavaSparkContext(sparkSession);
    }

    @Test
    public void graphExtractor() {
        List<Result> resultList = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            List<Cell> cellList = new ArrayList<>();
            cellList.add(CellUtil.createCell(Bytes.toBytes(String.valueOf(i)), Bytes.toBytes("I"), Bytes.toBytes("R"),
                    new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes(String.valueOf(i)), Bytes.toBytes("1")));
            cellList.add(CellUtil.createCell(Bytes.toBytes(String.valueOf(i)), Bytes.toBytes("D"),
                    Bytes.toBytes(String.valueOf((i + 1) % 4)),
                    new Date().getTime(), KeyValue.Type.Put, Bytes.toBytes(String.valueOf(i)), Bytes.toBytes("1")));
            resultList.add(Result.create(cellList));
        }
        HBaseSiteConfig hBaseSiteConfig = HBaseSiteConfig.load();
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
}
