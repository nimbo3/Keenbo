package in.nimbo.service;

import in.nimbo.common.config.HBaseSiteConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.SiteGraphConfig;
import in.nimbo.entity.Edge;
import in.nimbo.entity.GraphEdge;
import in.nimbo.entity.GraphNode;
import in.nimbo.entity.Node;
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
import java.util.Objects;

public class GraphExtractor {
    private GraphExtractor() {
    }

    public static void extract(HBaseSiteConfig hBaseSiteConfig, SiteGraphConfig siteGraphConfig,
                               SparkSession spark) {
        String siteTable = hBaseSiteConfig.getSiteTable();
        byte[] infoColumnFamily = hBaseSiteConfig.getInfoColumnFamily();
        byte[] domainColumnFamily = hBaseSiteConfig.getDomainColumnFamily();
        byte[] countColumn = hBaseSiteConfig.getCountColumn();
        byte[] siteRankColumn = hBaseSiteConfig.getRankColumn();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, siteTable);
        hBaseConfiguration.set(TableInputFormat.SCAN_BATCHSIZE, siteGraphConfig.getScanBatchSize());

        JavaRDD<Result> hBaseRDD = spark.sparkContext()
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                        , ImmutableBytesWritable.class, Result.class).toJavaRDD()
                .map(tuple -> tuple._2);
        hBaseRDD.persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<GraphNode> nodes = hBaseRDD
                .map(result -> result.getColumnLatestCell(infoColumnFamily, siteRankColumn))
                .filter(Objects::nonNull)
                .map(cell -> {
                    String rankStr = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    return new GraphNode(
                            Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()),
                            Double.parseDouble(rankStr)
                    );
                });

        JavaRDD<GraphEdge> edges = hBaseRDD
                .flatMap(result -> result.listCells().iterator())
                .filter(cell -> CellUtil.matchingFamily(cell, domainColumnFamily))
                .map(cell -> {
                    String from = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String to = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    long weight = Long.parseLong(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    return new GraphEdge(from, to, weight);
                });
        hBaseRDD.unpersist();

        Dataset<Row> vertexDF = spark.createDataFrame(nodes, GraphNode.class);
        Dataset<Row> edgeDF = spark.createDataFrame(edges, GraphEdge.class);

        vertexDF.repartition(1).write().json("/outputVertices");
        edgeDF.repartition(1).write().json("/outputEdges");

        spark.stop();
    }
}
