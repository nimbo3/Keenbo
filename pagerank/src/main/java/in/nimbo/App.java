package in.nimbo;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.PageRankConfig;
import in.nimbo.entity.Page;
import in.nimbo.entity.Relation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.impl.GraphImpl;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import scala.Tuple2;
import scala.collection.immutable.Map;

import java.util.NavigableMap;
import java.util.Set;

public class App {
    public static void main(String[] args) {
        HBaseConfig hBaseConfig = HBaseConfig.load();

        byte[] anchorColumnFamily = hBaseConfig.getAnchorColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, hBaseConfig.getLinksTable());
        hBaseConfiguration.set(TableInputFormat.SCAN_BATCHSIZE, "2000");

        SparkSession spark = SparkSession.builder()
                .appName("pagerank")
                .master("local")
                .getOrCreate();

        JavaRDD<Result> hBaseRDD = spark.sparkContext()
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                        , ImmutableBytesWritable.class, Result.class).toJavaRDD()
                .map(tuple -> tuple._2);

        JavaRDD<Page> nodes = hBaseRDD.filter(result -> result.getValue(Bytes.toBytes("R"), Bytes.toBytes("R")) != null)
                .map(result -> {
                    Page page = new Page();
                    page.setId(Bytes.toString(result.getRow()));
                    page.setPagerank(Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("R"), Bytes.toBytes("R")))));
                    return page;
                });

        JavaRDD<Relation> edges = hBaseRDD
                .flatMap(result -> result.getFamilyMap(anchorColumnFamily).entrySet().stream().map(
                        entry -> {
                            Relation relation = new Relation();
                            relation.setSrc(Bytes.toString(entry.getKey()));
                            relation.setDst(Bytes.toString(entry.getValue()));
                            return relation;
                        })
                        .iterator());

        Dataset<Row> verDF = spark.createDataFrame(nodes, Page.class);

        Dataset<Row> edgDF = spark.createDataFrame(edges, Relation.class);

        GraphFrame graphFrame = new GraphFrame(verDF, edgDF);
        GraphFrame pageRank = graphFrame.pageRank().maxIter(20).resetProbability(0.01).run();
        pageRank.vertices().show();

        spark.stop();

            /*JavaPairRDD<Set<byte[]>, Double> map = hBaseRDD
                    .mapToPair(result -> {
                        double rank = Bytes.toDouble(result.getValue(Bytes.toBytes("R"), Bytes.toBytes("R")));
                        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("A"));
                        int count = familyMap.keySet().size();
                        rank /= count;
                        return new Tuple2<>(familyMap.keySet(), rank);
                    });

            JavaPairRDD<byte[], Double> flatMapToPair = map.flatMapToPair(setRank -> setRank._1.stream().map(bytes ->
                    new Tuple2<>(bytes, setRank._2)).iterator());

            JavaPairRDD<byte[], Double> reduced = flatMapToPair.reduceByKey((v1, v2) -> (v1 + v2));
            reduced.saveAsTextFile("result.txt");*/


    }
}
