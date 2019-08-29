package in.nimbo;

import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.WordGraphConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import static in.nimbo.common.utility.LinkUtility.*;
import static org.apache.spark.sql.functions.*;

public class App {
    public static void main(String[] args) {
        HBasePageConfig hBaseConfig = HBasePageConfig.load();
        WordGraphConfig wordGraphConfig = WordGraphConfig.load();
        byte[] rankColumn = hBaseConfig.getRankColumn();
        byte[] anchorColumnFamily = hBaseConfig.getAnchorColumnFamily();
        byte[] dataColumnFamily = hBaseConfig.getDataColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, "fake");

        SparkSession spark = SparkSession.builder()
                .appName(wordGraphConfig.getAppName())
                .config("spark.hadoop.validateOutputSpecs", false)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryoserializer.buffer", "1024k")
                .getOrCreate();

        spark.sparkContext().conf().registerKryoClasses(new Class[]{
                in.nimbo.common.monitoring.ThreadsMonitor.class, in.nimbo.common.entity.Page.class,
                in.nimbo.common.entity.Anchor.class, in.nimbo.common.entity.Meta.class,
                in.nimbo.common.utility.LinkUtility.class, in.nimbo.common.utility.CloseUtility.class,
                in.nimbo.common.exception.HBaseException.class, in.nimbo.common.exception.InvalidLinkException.class,
                in.nimbo.common.exception.HashException.class, in.nimbo.common.exception.LoadConfigurationException.class,
                in.nimbo.common.exception.ElasticException.class, in.nimbo.common.exception.LanguageDetectException.class,
                in.nimbo.common.exception.ParseLinkException.class, in.nimbo.common.exception.ReverseLinkException.class,
                in.nimbo.common.serializer.PageDeserializer.class, in.nimbo.common.serializer.PageSerializer.class,
                in.nimbo.common.config.RedisConfig.class, in.nimbo.common.config.KafkaConfig.class,
                in.nimbo.common.config.ElasticConfig.class, in.nimbo.common.config.ProjectConfig.class,
                in.nimbo.common.config.Config.class, in.nimbo.common.config.HBaseSiteConfig.class,
                in.nimbo.common.config.HBasePageConfig.class, in.nimbo.App.class, in.nimbo.config.WordGraphConfig.class
        });

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

        JavaRDD<Row> pageContentRDD = hBaseCellsRDD
                .filter(cell -> CellUtil.matchingFamily(cell, dataColumnFamily))
                .filter(cell -> !CellUtil.matchingQualifier(cell, rankColumn))
                .map(cell -> RowFactory.create(
                        Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()),
                        Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())));

        JavaRDD<Row> edges = hBaseCellsRDD
                .filter(cell -> CellUtil.matchingFamily(cell, anchorColumnFamily))
                .map(cell -> {
                    String source = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String destination = Bytes.toString(cell.getQualifierArray()
                            , cell.getQualifierOffset(), cell.getQualifierLength());
                    int index = destination.indexOf('#');
                    if (index != -1)
                        destination = destination.substring(0, index);
                    return RowFactory.create(source, LinkUtility.reverseLink(destination));
                }).filter(domain -> !domain.getString(0).equals(domain.getString(1)));

        StructType pageNodeSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("content", DataTypes.StringType, false, Metadata.empty()),
        });

        StructType edgesSchema = new StructType(new StructField[]{
                new StructField("src", DataTypes.StringType, false, Metadata.empty()),
                new StructField("dst", DataTypes.StringType, false, Metadata.empty()),
        });

        Dataset<Row> pageVertexDF = spark.createDataFrame(pageContentRDD, pageNodeSchema);
        Dataset<Row> edgeDF = spark.createDataFrame(edges, edgesSchema);
        edgeDF.repartition(32);
        hBaseCellsRDD.unpersist();

        pageVertexDF = pageVertexDF.groupBy(col("id")).agg(collect_list("content").alias("keywords"));
        GraphFrame graphFrame = new GraphFrame(pageVertexDF, edgeDF);
        Dataset<Row> keywords = graphFrame
                .triplets()
                .groupBy("src", "dst")
                .agg(functions.count(functions.lit(1)).alias("weight"));

        keywords = keywords.withColumn("word1", explode(col("src.keywords")))
                .withColumn("word2", explode(col("dst.keywords")))
                .select("word1", "word2", "weight")
                .filter(col("word1").notEqual(col("word2")));
        keywords.repartition(32);

        keywords = keywords.select(least("word1", "word2").alias("from")
                , greatest("word1", "word2").alias("to"), col("weight"))
                .groupBy("from", "to")
                .agg(functions.sum("weight").alias("width"))
                .sort(desc("width")).limit(1000000);

        Dataset<Row> verticesPart1 = keywords.select(col("from")).dropDuplicates();
        Dataset<Row> verticesPart2 = keywords.select(col("to")).dropDuplicates();
        Dataset<Row> vertices = verticesPart1.union(verticesPart2)
                .select(col("from").as("id")).dropDuplicates();

        saveAsJson(keywords, "/wordGraphEdges");
        saveAsJson(vertices, "/wordGraphVertices");

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
