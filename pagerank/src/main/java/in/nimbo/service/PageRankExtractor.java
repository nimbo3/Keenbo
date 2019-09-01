package in.nimbo.service;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.PageRankConfig;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;
import in.nimbo.entity.Page;
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
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;
import scala.Tuple2;

public class PageRankExtractor {

    private PageRankExtractor() {
    }

    public static Tuple2<JavaPairRDD<ImmutableBytesWritable, Put>, JavaRDD<Page>> extract(HBaseConfig hBasePageConfig, PageRankConfig pageRankConfig,
                                                                                          SparkSession spark, JavaRDD<Result> hBaseRDD) {
        byte[] dataColumnFamily = hBasePageConfig.getDataColumnFamily();
        byte[] rankColumn = hBasePageConfig.getPageRankColumn();
        byte[] anchorColumnFamily = hBasePageConfig.getAnchorColumnFamily();

        JavaRDD<Node> nodes = hBaseRDD.map(result -> new Node(Bytes.toString(result.getRow())));
        JavaRDD<Edge> edges = hBaseRDD.flatMap(result -> result.listCells().iterator())
                .filter(cell -> CellUtil.matchingFamily(cell, anchorColumnFamily))
                .map(cell -> new Edge(
                        Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()),
                        LinkUtility.reverseLink(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()))
                ));

        Dataset<Row> vertexDF = spark.createDataFrame(nodes, Node.class);
        Dataset<Row> edgeDF = spark.createDataFrame(edges, Edge.class);
        edgeDF.repartition(32);
        vertexDF.persist(StorageLevel.DISK_ONLY());
        edgeDF.persist(StorageLevel.DISK_ONLY());
        hBaseRDD.unpersist();

        GraphFrame graphFrame = new GraphFrame(vertexDF, edgeDF);
        graphFrame.persist(StorageLevel.DISK_ONLY());
        vertexDF.unpersist();
        edgeDF.unpersist();
        GraphFrame pageRank = graphFrame.pageRank().maxIter(pageRankConfig.getMaxIter()).
                resetProbability(pageRankConfig.getResetProbability()).run();
        pageRank.persist(StorageLevel.DISK_ONLY());
        graphFrame.unpersist();
        JavaRDD<Row> pageRankRdd = pageRank.vertices().toJavaRDD();
        pageRankRdd.persist(StorageLevel.DISK_ONLY());

        JavaPairRDD<ImmutableBytesWritable, Put> javaPairRDD = pageRankRdd.mapToPair(row -> {
            Put put = new Put(Bytes.toBytes(row.getString(0)));
            put.addColumn(dataColumnFamily, rankColumn, Bytes.toBytes(String.valueOf(row.getDouble(1))));
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });

        JavaRDD<Page> esPageJavaRDD = pageRankRdd
                .map(row -> new Page(
                        LinkUtility.hashLink(LinkUtility.reverseLink(row.getString(0))),
                        row.getDouble(1)));

        return new Tuple2<>(javaPairRDD, esPageJavaRDD);
    }
}
