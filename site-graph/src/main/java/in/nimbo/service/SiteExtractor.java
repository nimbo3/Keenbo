package in.nimbo.service;

import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.config.HBaseSiteConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;
import scala.Tuple2;

import java.util.Objects;

public class SiteExtractor {
    private SiteExtractor() {
    }

    public static Tuple2<JavaPairRDD<ImmutableBytesWritable, Put>, JavaPairRDD<ImmutableBytesWritable, Put>>
    extract(HBasePageConfig hBasePageConfig, HBaseSiteConfig hBaseSiteConfig,
            SparkSession spark, JavaRDD<Result> hBaseRDD) {
        byte[] infoColumnFamily = hBaseSiteConfig.getInfoColumnFamily();
        byte[] domainColumnFamily = hBaseSiteConfig.getDomainColumnFamily();
        byte[] countColumn = hBaseSiteConfig.getCountColumn();
        byte[] siteRankColumn = hBaseSiteConfig.getRankColumn();

        byte[] dataColumnFamily = hBasePageConfig.getDataColumnFamily();
        byte[] pageRankColumn = hBasePageConfig.getRankColumn();
        byte[] anchorColumnFamily = hBasePageConfig.getAnchorColumnFamily();

        JavaRDD<Node> nodes = hBaseRDD
                .map(result -> result.getColumnLatestCell(dataColumnFamily, pageRankColumn))
                .filter(Objects::nonNull)
                .map(cell -> {
                    String rankStr = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    return new Node(
                            LinkUtility.getMainDomainForReversed(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())),
                            Double.parseDouble(rankStr)
                    );
                }).distinct();

        JavaRDD<Edge> edges = hBaseRDD.flatMap(result -> result.listCells().iterator()).
                filter(cell -> CellUtil.matchingFamily(cell, anchorColumnFamily)).
                mapToPair(cell -> {
                    String destination = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    int index = destination.indexOf('#');
                    if (index != -1)
                        destination = destination.substring(0, index);
                    return new Tuple2<>(
                            Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()),
                            destination);
                }).map(link -> new Edge(LinkUtility.getMainDomainForReversed(link._1), LinkUtility.getMainDomain(link._2))).
                filter(domain -> !domain.getDst().equals(domain.getSrc()));

        hBaseRDD.unpersist();

        Dataset<Row> vertexDF = spark.createDataFrame(nodes, Node.class);
        Dataset<Row> edgeDF = spark.createDataFrame(edges, Edge.class);
        edgeDF.repartition(32);

        Dataset<Row> vertices = vertexDF.groupBy("id").agg(functions.avg("rank"), functions.count(functions.lit(1)).alias("count"));
        vertices.repartition(32);
        JavaRDD<Row> verticesRDD = vertices.toJavaRDD();
        verticesRDD.persist(StorageLevel.MEMORY_AND_DISK());

        GraphFrame graphFrame = new GraphFrame(vertexDF.select("id"), edgeDF);
        Dataset<Row> edgesWithWeight = graphFrame
                .triplets()
                .groupBy("src", "dst")
                .agg(functions.count(functions.lit(1)).alias("weight"));
        edgesWithWeight.repartition(32);

        JavaRDD<Row> edgesWithWeightRdd = edgesWithWeight.toJavaRDD();
        edgesWithWeightRdd.persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<ImmutableBytesWritable, Put> verticesPut = verticesRDD.mapToPair(row -> {
            Put put = new Put(Bytes.toBytes(row.getString(0)));
            put.addColumn(infoColumnFamily, siteRankColumn, Bytes.toBytes(String.valueOf(row.getDouble(1))));
            put.addColumn(infoColumnFamily, countColumn, Bytes.toBytes(String.valueOf(row.getLong(2))));
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });

        JavaPairRDD<ImmutableBytesWritable, Put> edgesSCCPut = edgesWithWeightRdd.mapToPair(row -> {
            Put put = new Put(Bytes.toBytes(row.getStruct(0).getString(0)));
            put.addColumn(domainColumnFamily, Bytes.toBytes(row.getStruct(1).getString(0)),
                    Bytes.toBytes(String.valueOf(row.getLong(2))));
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });

        return new Tuple2<>(verticesPut, edgesSCCPut);
    }
}
