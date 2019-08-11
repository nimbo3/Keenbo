package in.nimbo;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.AppConfig;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;
import in.nimbo.entity.Page;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.graphframes.GraphFrame;

import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.count;


public class App {
    public static void main(String[] args) {
        AppConfig appConfig = AppConfig.load();
        HBaseConfig hBaseConfig = HBaseConfig.load();

        byte[] anchorColumnFamily = hBaseConfig.getAnchorColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, hBaseConfig.getLinksTable());
        hBaseConfiguration.set(TableInputFormat.SCAN_BATCHSIZE, appConfig.getScanBatchSize());

        SparkSession spark = SparkSession.builder()
                .appName(appConfig.getAppName())
                .master(appConfig.getResourceManager())
                .getOrCreate();
        spark.conf().set("es.nodes", appConfig.getNodesIP());
        spark.conf().set("es.write.operation", "upsert");
        spark.conf().set("es.mapping.id", "id");
        spark.conf().set("es.index.auto.create", appConfig.getEsCreateIndex());

        JavaRDD<Result> hBaseRDD = spark.sparkContext()
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                        , ImmutableBytesWritable.class, Result.class).toJavaRDD()
                .map(tuple -> tuple._2);

        JavaRDD<Node> nodes = hBaseRDD
                .map(result -> new Node(Bytes.toString(result.getRow())));

        JavaRDD<Edge> edges = hBaseRDD
                .flatMap(result -> result.getFamilyMap(anchorColumnFamily).entrySet().stream().map(
                        entry -> new Edge(
                                Bytes.toString(result.getRow()),
                                LinkUtility.reverseLink(Bytes.toString(entry.getKey()).substring(0, Bytes.toString(entry.getKey()).indexOf("#"))),
                                Bytes.toString(entry.getValue())))
                        .iterator());

        Dataset<Row> verDF = spark.createDataFrame(nodes, Node.class);

        Dataset<Row> edgDF = spark.createDataFrame(edges, Edge.class);

        GraphFrame graphFrame = new GraphFrame(verDF, edgDF);
        Dataset<Row> anchors = graphFrame.triplets().groupBy("dst")
                .agg(collect_set("edge.anchor").alias("anchors"), count("edge.anchor").alias("count"));

        JavaRDD<Page> anchorsRDD = anchors.toJavaRDD()
                .map(row -> new Page(
                        LinkUtility.hashLink(LinkUtility.reverseLink(row.getStruct(0).getString(0))),
                        row.getList(1), row.getLong(2)));

        JavaEsSpark.saveToEs(anchorsRDD, appConfig.getEsIndexName() + "/" + appConfig.getEsType());

        spark.stop();
    }
}
