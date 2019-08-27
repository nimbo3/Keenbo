package in.nimbo;

import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.WordGraphConfig;
import in.nimbo.entity.Edge;
import in.nimbo.entity.PageNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static in.nimbo.common.utility.LinkUtility.getMainDomain;
import static in.nimbo.common.utility.LinkUtility.getMainDomainForReversed;

public class App {
    public static void main(String[] args) {
        HBasePageConfig hBaseConfig = HBasePageConfig.load();
        WordGraphConfig wordGraphConfig = WordGraphConfig.load();
        byte[] rankColumn = hBaseConfig.getDataColumnFamily();
        byte[] anchorColumnFamily = hBaseConfig.getAnchorColumnFamily();
        byte[] dataColumnFamily = hBaseConfig.getDataColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, "fake");

        SparkSession spark = SparkSession.builder()
                .appName(wordGraphConfig.getAppName())
                .getOrCreate();

        spark.sparkContext().conf().set("spark.speculation", "false");
        spark.sparkContext().conf().set("spark.hadoop.mapreduce.map.speculative", "false");
        spark.sparkContext().conf().set("spark.hadoop.mapreduce.reduce.speculative", "false");
        spark.sparkContext().conf().set("spark.kryo.registrationRequired", "true");

        JavaRDD<Cell> hBaseCellsRDD = spark.sparkContext()
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class).toJavaRDD()
                .map(tuple -> tuple._2)
                .flatMap(result -> result.listCells().iterator());
        hBaseCellsRDD.persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<String, Iterable<String>> pageWordPairRDD = hBaseCellsRDD
                .filter(cell -> CellUtil.matchingFamily(cell, dataColumnFamily))
                .filter(cell -> !(CellUtil.matchingQualifier(cell, rankColumn)))
                .mapToPair(cell -> new Tuple2<>(Bytes.toString(CellUtil.cloneRow(cell)),
                        Bytes.toString(CellUtil.cloneQualifier(cell))))
                .groupByKey();

        JavaRDD<PageNode> pageNodeRDD = pageWordPairRDD.map(pair -> new PageNode(pair._1, pair._2));

        JavaPairRDD<String, String> links = hBaseCellsRDD
                .filter(cell -> Arrays.equals(CellUtil.cloneFamily(cell), anchorColumnFamily))
                .mapToPair(cell -> {
                    String destination = Bytes.toString(CellUtil.cloneQualifier(cell));
                    int index = destination.indexOf('#');
                    if (index != -1)
                        destination = destination.substring(0, index);
                    return new Tuple2<>(Bytes.toString(CellUtil.cloneRow(cell)), destination);
                });

        JavaRDD<Edge> edges = links
                .mapToPair(link -> new Tuple2<>(getMainDomainForReversed(link._1), getMainDomain(link._2)))
                .filter(domain -> !domain._1.equals(domain._2))
                .map(link -> new Edge(link._1, link._2));

        Dataset<Row> pageVerDF = spark.createDataFrame(pageNodeRDD, PageNode.class);
        pageVerDF.show(false);
        Dataset<Row> edgeDF = spark.createDataFrame(pageNodeRDD, PageNode.class);
        edgeDF.show(false);
        GraphFrame graphFrame = new GraphFrame(pageVerDF.select("id"), edgeDF);
        Dataset<Row> edgesWithWeight = graphFrame
                .triplets()
                .groupBy("src", "dst")
                .agg(functions.count(functions.lit(1)).alias("weight"));
        edgesWithWeight.repartition(32);
        edgesWithWeight.show(false);

        spark.stop();
    }
}
