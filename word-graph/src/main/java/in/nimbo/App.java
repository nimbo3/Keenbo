package in.nimbo;

import in.nimbo.common.config.HBasePageConfig;
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
    public static void main(String[] args) throws ClassNotFoundException, IOException {
        final List<Class<?>> classes = new ArrayList<Class<?>>();
        final JarInputStream jarFile = new JarInputStream(new FileInputStream("word-graph/target/word-graph-1.0-jar-with-dependencies.jar"));
        JarEntry jarEntry = null;
        do {
            jarEntry = jarFile.getNextJarEntry();
            if (jarEntry != null) {
                String className = jarEntry.getName();
                if (className.endsWith(".class")) {
                    className = className.substring(0, className.lastIndexOf('.')); // strip filename extension
                    if (className.startsWith("in" + "/")) {  // match classes in the specified package and its subpackages
                        classes.add(Class.forName(className.replace('/', '.')));
                    }
                }
            }
        } while (jarEntry != null);
        System.out.println(classes);
        System.exit(0);


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
                .master("local")
                .getOrCreate();

        spark.sparkContext().conf().set("spark.speculation", "false");
        spark.sparkContext().conf().set("spark.hadoop.mapreduce.map.speculative", "false");
        spark.sparkContext().conf().set("spark.hadoop.mapreduce.reduce.speculative", "false");

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

        StructType pageNodeSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("content", DataTypes.StringType, false, Metadata.empty()),
        });

        JavaPairRDD<String, String> links = hBaseCellsRDD
                .filter(cell -> CellUtil.matchingFamily(cell, anchorColumnFamily))
                .mapToPair(cell -> {
                    String destination = Bytes.toString(cell.getQualifierArray()
                            , cell.getQualifierOffset(), cell.getQualifierLength());
                    int index = destination.indexOf('#');
                    if (index != -1)
                        destination = destination.substring(0, index);
                    return new Tuple2<>(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                            , destination);
                });

        JavaRDD<Row> edges = links
                .mapToPair(link -> new Tuple2<>(link._1, reverseLink(link._2)))
                .filter(domain -> !domain._1.equals(domain._2))
                .map(link -> RowFactory.create(link._1, link._2));

        StructType edgesSchema = new StructType(new StructField[]{
                new StructField("src", DataTypes.StringType, false, Metadata.empty()),
                new StructField("dst", DataTypes.StringType, false, Metadata.empty()),
        });

        Dataset<Row> pageVertexDF = spark.createDataFrame(pageContentRDD, pageNodeSchema);
        Dataset<Row> edgeDF = spark.createDataFrame(edges, edgesSchema);
        pageVertexDF = pageVertexDF.groupBy(col("id")).agg(collect_list("content").alias("keywords"));

        pageVertexDF.show();
        edgeDF.show();
        GraphFrame graphFrame = new GraphFrame(pageVertexDF, edgeDF);
        Dataset<Row> e = graphFrame
                .triplets()
                .groupBy("src", "dst")
                .agg(functions.count(functions.lit(1)).alias("width"));
        e.repartition(32);
        e = e.select(col("src.keywords").alias("w1"), col("dst.keywords").alias("w2"), col("width").alias("width"));
        e = e.withColumn("word1", explode(col("w1")));
        e = e.withColumn("word2", explode(col("w2")))
                .select("word1", "word2", "width")
                .filter(col("word1").notEqual(col("word2")));

        e = e.select(least("word1", "word2").as("from")
                , greatest("word1", "word2").alias("to"), col("width"));

        e = e.groupBy("from", "to")
                .agg(functions.sum("width").alias("width")).sort(desc("width")).limit(1000000);

        Dataset<Row> verticesPart1 = e.select(col("from")).distinct().dropDuplicates();
        Dataset<Row> verticesPart2 = e.select(col("to")).distinct().dropDuplicates();
        Dataset<Row> vertices = verticesPart1.union(verticesPart2).distinct()
                .select(col("from").as("id")).dropDuplicates();
        vertices.show(false);

        saveAsJson(e, "/wordGraph");
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
