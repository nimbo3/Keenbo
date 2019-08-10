package in.nimbo;

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
        PageRankConfig pageRankConfig = PageRankConfig.load();
        SparkConf sparkConf = new SparkConf()
                .setAppName("pagerank");
        SparkSession spark = SparkSession.builder().appName("pagerank").master("local").getOrCreate();


        String columnFamily = pageRankConfig.getHbaseColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, pageRankConfig.getHbaseTable());
        hBaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, columnFamily);


        try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<Result> hBaseRDD = javaSparkContext
                    .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                            , ImmutableBytesWritable.class, Result.class).values();

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
            JavaRDD<String> vertexes = hBaseRDD.map(result -> Bytes.toString(result.getRow()));
            JavaRDD<Relation> edges = hBaseRDD.flatMapToPair(result -> result.getFamilyMap(Bytes.toBytes("A")).keySet().stream().map(bytes -> {
                return new Tuple2<>(Bytes.toString(result.getRow()), Bytes.toString(bytes));
            }).iterator()).map(stringStringTuple2 -> {
                Relation relation = new Relation();
                relation.setSrc(stringStringTuple2._1);
                relation.setDst(stringStringTuple2._2);
                relation.setRelationship("forward");
                return relation;
            });
            Dataset<Row> vertexesDF = spark.createDataFrame(vertexes, Page.class);
            Dataset<Row> edgesDF = spark.createDataFrame(edges, Relation.class);
            GraphFrame graphFrame = new GraphFrame(vertexesDF, edgesDF);
            GraphFrame run = graphFrame.pageRank().resetProbability(0.01).maxIter(20).run();
            run.vertices().select("id", "pagerank").show();
        }
    }
}
