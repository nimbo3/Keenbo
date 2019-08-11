package in.nimbo;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.entity.Page;
import in.nimbo.entity.Relation;
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
import org.graphframes.GraphFrame;
import scala.Tuple2;

import java.io.IOException;

public class App {
    public static void main(String[] args) {
        HBaseConfig hBaseConfig = HBaseConfig.load();

        byte[] anchorColumnFamily = hBaseConfig.getAnchorColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, hBaseConfig.getLinksTable());

        SparkSession spark = SparkSession.builder()
                .config("spark.hadoop.validateOutputSpecs", false)
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
                    return page;
                });

        JavaRDD<Relation> edges = hBaseRDD
                .flatMap(result -> result.getFamilyMap(anchorColumnFamily).keySet().stream().map(
                        entry -> {
                            Relation relation = new Relation();
                            relation.setSrc(Bytes.toString(result.getRow()));
                            relation.setDst(LinkUtility.reverseLink(Bytes.toString(entry)));
                            return relation;
                        })
                        .iterator());

        Dataset<Row> verDF = spark.createDataFrame(nodes, Page.class);

        Dataset<Row> edgDF = spark.createDataFrame(edges, Relation.class);

        GraphFrame graphFrame = new GraphFrame(verDF, edgDF);
        GraphFrame pageRank = graphFrame.pageRank().maxIter(1).resetProbability(0.01).run();
        pageRank.vertices().sort("pagerank").show(2000, false);
        JavaRDD<Row> pageRankRdd = pageRank.vertices().toJavaRDD();
        JavaPairRDD<ImmutableBytesWritable, Put> javaPairRDD = pageRankRdd.mapToPair(row -> {
            Put put = new Put(Bytes.toBytes(row.getString(0)));
            put.addColumn(Bytes.toBytes("R"), Bytes.toBytes("R"), Bytes.toBytes(String.valueOf(row.getDouble(1))));
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });
        try {
            Job jobConf = Job.getInstance();
            jobConf.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "dummy_page");
            jobConf.setOutputFormatClass(TableOutputFormat.class);
            jobConf.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
            javaPairRDD.saveAsNewAPIHadoopDataset(jobConf.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }

        spark.stop();
    }
}
