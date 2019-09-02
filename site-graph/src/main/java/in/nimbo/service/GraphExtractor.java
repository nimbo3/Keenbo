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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;
import org.graphframes.lib.StronglyConnectedComponents;

import java.util.List;
import java.util.Objects;

import static org.apache.spark.sql.functions.col;

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
        bestNodesRdd.persist(StorageLevel.DISK_ONLY());

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

        StructType nodeSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("font", DataTypes.createStructType(
                        new StructField[]{
                                new StructField("size", DataTypes.DoubleType, false, Metadata.empty())
                        }
                ), false, Metadata.empty())});

        StructType edgeSchema = new StructType(new StructField[]{
                new StructField("src", DataTypes.StringType, false, Metadata.empty()),
                new StructField("dst", DataTypes.StringType, false, Metadata.empty()),
                new StructField("width", DataTypes.LongType, false, Metadata.empty())
        });

        Dataset<Row> vertexDF = spark.createDataFrame(nodes, nodeSchema);
        Dataset<Row> edgeDF = spark.createDataFrame(edges, edgeSchema);

        spark.sparkContext().setCheckpointDir("w");

        GraphFrame graphFrame = new GraphFrame(vertexDF, edgeDF);
        graphFrame.persist(StorageLevel.DISK_ONLY());
        Dataset<Row> components = graphFrame.connectedComponents().setIntermediateStorageLevel(StorageLevel.DISK_ONLY()).run();

        Dataset<Row> finalVertices = components.select(col("id"), col("font"), col("component").alias("color"));
        Dataset<Row> finalEdges = edgeDF.select(col("src").alias("from"), col("dst").alias("to"), col("width"));
        return new GraphResult(finalVertices, finalEdges);
    }
}
