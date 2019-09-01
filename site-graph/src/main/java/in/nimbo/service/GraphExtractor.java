package in.nimbo.service;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.entity.GraphResult;
import in.nimbo.common.utility.SparkUtility;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.util.List;
import java.util.Objects;

public class GraphExtractor {
    private GraphExtractor() {
    }

    public static GraphResult extract(HBaseConfig hBaseConfig, SparkSession spark, JavaRDD<Result> hBaseRDD) {
        JavaSparkContext javaSparkContext = SparkUtility.getJavaSparkContext(spark);
        byte[] infoColumnFamily = hBaseConfig.getInfoColumnFamily();
        byte[] domainColumnFamily = hBaseConfig.getDomainColumnFamily();
        byte[] siteRankColumn = hBaseConfig.getSiteRankColumn();

        List<Result> bestNodes = hBaseRDD
                .filter(result -> result.getColumnLatestCell(infoColumnFamily, siteRankColumn) != null)
                .sortBy(result -> {
                    Cell rankCell = result.getColumnLatestCell(infoColumnFamily, siteRankColumn);
                    String rank = Bytes.toString(rankCell.getValueArray(), rankCell.getValueOffset(), rankCell.getValueLength());
                    return Double.parseDouble(rank);
                }, false, 32).take(1000);

        hBaseRDD.unpersist();
        JavaRDD<Result> bestNodesRdd = javaSparkContext.parallelize(bestNodes);
        bestNodesRdd.persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<Row> nodes = bestNodesRdd
                .map(result -> result.getColumnLatestCell(infoColumnFamily, siteRankColumn))
                .filter(Objects::nonNull)
                .map(cell -> {
                    String rankStr = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    return RowFactory.create(
                            Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()),
                            RowFactory.create(Double.parseDouble(rankStr))
                    );
                });

        JavaRDD<Row> edges = bestNodesRdd
                .flatMap(result -> result.listCells().iterator())
                .filter(cell -> CellUtil.matchingFamily(cell, domainColumnFamily))
                .map(cell -> {
                    String from = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String to = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    long weight = Long.parseLong(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    return RowFactory.create(from, to, weight);
                });
        hBaseRDD.unpersist();

        Dataset<Row> vertexDF = spark.createDataFrame(nodes, SparkUtility.getNodeSchema());
        Dataset<Row> edgeDF = spark.createDataFrame(edges, SparkUtility.getEdgeSchema());

        return new GraphResult(vertexDF, edgeDF);
    }
}
