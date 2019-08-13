package in.nimbo;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.PageRankConfig;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;
import in.nimbo.entity.Page;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.graphframes.GraphFrame;
import scala.Tuple2;

import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException {
        HBaseConfig hBaseConfig = HBaseConfig.load();
        PageRankConfig pageRankConfig = PageRankConfig.load();
        String esIndex = pageRankConfig.getEsIndex();
        String esType = pageRankConfig.getEsType();
        byte[] rankColumn = hBaseConfig.getRankColumnFamily();
        byte[] anchorColumnFamily = hBaseConfig.getAnchorColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, hBaseConfig.getLinksTable());

        SparkSession spark = SparkSession.builder()
                .config("spark.hadoop.validateOutputSpecs", false)
                .appName(pageRankConfig.getAppName())
                .master(pageRankConfig.getResourceManager())
                .getOrCreate();
        spark.sparkContext().conf().set("es.nodes", pageRankConfig.getEsNodes());
        spark.sparkContext().conf().set("es.write.operation", pageRankConfig.getEsWriteOperation());
        spark.sparkContext().conf().set("es.mapping.id", "id");
        spark.sparkContext().conf().set("es.index.auto.create", pageRankConfig.getEsIndexAutoCreate());

        JavaRDD<Result> hBaseRDD = spark.sparkContext()
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                        , ImmutableBytesWritable.class, Result.class).toJavaRDD()
                .map(tuple -> tuple._2);

        JavaRDD<Node> nodes = hBaseRDD.map(result -> new Node(Bytes.toString(result.getRow())));
        JavaRDD<Edge> edges = hBaseRDD
                .flatMap(result -> result.getFamilyMap(anchorColumnFamily).keySet().stream().map(
                        entry -> new Edge(
                                Bytes.toString(result.getRow()),
                                LinkUtility.reverseLink(Bytes.toString(entry))
                        )).iterator());

        Dataset<Row> vertexDF = spark.createDataFrame(nodes, Node.class);
        Dataset<Row> edgeDF = spark.createDataFrame(edges, Edge.class);

        GraphFrame graphFrame = new GraphFrame(vertexDF, edgeDF);
        GraphFrame pageRank = graphFrame.pageRank().maxIter(pageRankConfig.getMaxIter()).resetProbability(pageRankConfig.getResetProbability()).run();

        JavaRDD<Row> pageRankRdd = pageRank.vertices().toJavaRDD();
        JavaPairRDD<ImmutableBytesWritable, Put> javaPairRDD = pageRankRdd.mapToPair(row -> {
            Put put = new Put(Bytes.toBytes(row.getString(0)));
            put.addColumn(rankColumn, rankColumn, Bytes.toBytes(String.valueOf(row.getDouble(1))));
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });

        JavaRDD<Page> esPageJavaRDD = pageRankRdd
                .map(row -> new Page(
                        LinkUtility.hashLink(LinkUtility.reverseLink(row.getString(0))),
                        row.getDouble(1)));

        Job jobConf = Job.getInstance();
        jobConf.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hBaseConfig.getLinksTable());
        jobConf.setOutputFormatClass(TableOutputFormat.class);
        jobConf.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
        javaPairRDD.saveAsNewAPIHadoopDataset(jobConf.getConfiguration());

        JavaEsSpark.saveToEs(esPageJavaRDD, esIndex + "/" + esType);

        spark.stop();
    }
}
