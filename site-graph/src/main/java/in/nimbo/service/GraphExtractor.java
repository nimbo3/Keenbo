package in.nimbo.service;

import in.nimbo.common.config.HBaseSiteConfig;
import in.nimbo.config.SiteGraphConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
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

import java.util.List;
import java.util.Objects;

public class GraphExtractor {
    private GraphExtractor() {
    }

    public static void extract(HBaseSiteConfig hBaseSiteConfig, SiteGraphConfig siteGraphConfig,
                               SparkSession spark) {
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
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

        List<Result> bestNodes = hBaseRDD
                .filter(result -> result.getColumnLatestCell(infoColumnFamily, siteRankColumn) != null)
                .sortBy(result -> {
                    Cell rankCell = result.getColumnLatestCell(infoColumnFamily, siteRankColumn);
                    String rank = Bytes.toString(rankCell.getValueArray(), rankCell.getValueOffset(), rankCell.getValueLength());
                    return Double.parseDouble(rank);
                }, false, 32).take(100);

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

        StructType nodesSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("font", DataTypes.createStructType(
                        new StructField[]{
                                new StructField("size", DataTypes.DoubleType, false, Metadata.empty())
                        }
                ), false, Metadata.empty())});

        StructType edgesSchema = new StructType(new StructField[]{
                new StructField("from", DataTypes.StringType, false, Metadata.empty()),
                new StructField("to", DataTypes.createStructType(
                        new StructField[]{
                                new StructField("width", DataTypes.DoubleType, false, Metadata.empty())
                        }
                ), false, Metadata.empty())});

        Dataset<Row> vertexDF = spark.createDataFrame(nodes, nodesSchema);
        Dataset<Row> edgeDF = spark.createDataFrame(edges, edgesSchema);

        saveAsJson(vertexDF, "/outputVertices");
        saveAsJson(edgeDF, "/outputEdges");

        spark.stop();
    }

    private static void saveAsJson(Dataset<Row> dataset, String path) {
        long count = dataset.count();
        dataset.toJSON().repartition(1)
                .javaRDD()
                .zipWithIndex()
                .map(val -> {
                    if (val._2 == 0) {
                        return "[\n" + val._1 + ",";
                    } else {
                        return val._2 == count - 1 ? val._1 + "\n]" : val._1 + ",";
                    }
                })
                .saveAsTextFile(path);
    }
}
