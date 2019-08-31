package in.nimbo.service;

import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;
import in.nimbo.entity.Page;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.count;

public class BackwardExtractorService {
    private BackwardExtractorService() {
    }

    public static JavaRDD<Page> extractBackward(HBasePageConfig hBasePageConfig, SparkSession spark, JavaRDD<Result> hBaseRDD) {
        byte[] anchorColumnFamily = hBasePageConfig.getAnchorColumnFamily();
        JavaRDD<Node> nodes = hBaseRDD.map(result -> new Node(Bytes.toString(result.getRow())));

        JavaRDD<Edge> edges = hBaseRDD.flatMap(result -> result.listCells().iterator())
                .filter(cell -> CellUtil.matchingFamily(cell, anchorColumnFamily))
                .map(cell -> {
                    String rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String anchorLink = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    int index = anchorLink.indexOf('#');
                    if (index != -1) {
                        anchorLink = anchorLink.substring(0, index);
                    }
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    return new Edge(rowKey, LinkUtility.reverseLink(anchorLink), value);
                });

        hBaseRDD.unpersist();

        Dataset<Row> verDF = spark.createDataFrame(nodes, Node.class);
        Dataset<Row> edgDF = spark.createDataFrame(edges, Edge.class);
        edgDF.repartition(32);

        GraphFrame graphFrame = new GraphFrame(verDF, edgDF);
        Dataset<Row> anchors = graphFrame.triplets().groupBy("dst")
                .agg(collect_set("edge.anchor").alias("anchors"), count("edge.anchor").alias("count"));

        JavaRDD<Page> anchorsRDD = anchors.toJavaRDD()
                .map(row -> new Page(
                        LinkUtility.hashLink(LinkUtility.reverseLink(row.getStruct(0).getString(0))),
                        row.getList(1), row.getLong(2)));

        return anchorsRDD;
    }
}
