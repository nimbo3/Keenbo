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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static in.nimbo.common.utility.LinkUtility.*;
import static org.apache.spark.sql.functions.*;

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

        JavaPairRDD<String, Iterable<String>> pageWordPairRDD = hBaseCellsRDD
                .filter(cell -> CellUtil.matchingFamily(cell, dataColumnFamily))
                .filter(cell -> !(CellUtil.matchingQualifier(cell, rankColumn)))
                .mapToPair(cell -> new Tuple2<>(
                        Bytes.toString(CellUtil.cloneRow(cell)),
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
                .mapToPair(link -> new Tuple2<>(link._1, reverseLink(link._2)))
                .filter(domain -> !domain._1.equals(domain._2))
                .map(link -> new Edge(link._1, link._2, 1));

        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

//        List<PageNode> pageNodes = new ArrayList<>();
//
//        List<String> keywords = new ArrayList<>();
//        keywords.add("nimbo");
//        keywords.add("sahab");
//        keywords.add("internship");
//        keywords.add("program");
//        pageNodes.add(new PageNode("nimbo", keywords));
//
//        keywords = new ArrayList<>();
//        keywords.add("stack");
//        keywords.add("overflow");
//        keywords.add("program");
//        keywords.add("question");
//        pageNodes.add(new PageNode("stackoverflow", keywords));
//
//        keywords = new ArrayList<>();
//        keywords.add("google");
//        keywords.add("search");
//        keywords.add("engine");
//        pageNodes.add(new PageNode("google", keywords));
//
//        JavaRDD<PageNode> pageNodeRDD = javaSparkContext.parallelize(pageNodes);

//        List<Edge> edg = new ArrayList<>();
//        edg.add(new Edge("google", "stackoverflow", 1));
//        edg.add(new Edge("google", "stackoverflow", 1));
//        edg.add(new Edge("google", "nimbo", 1));
//        edg.add(new Edge("nimbo", "stackoverflow", 1));
//
//        JavaRDD<Edge> edges = javaSparkContext.parallelize(edg);

        Dataset<Row> pageVerDF = spark.createDataFrame(pageNodeRDD, PageNode.class);
        pageVerDF.show(false);
        Dataset<Row> edgeDF = spark.createDataFrame(edges, Edge.class);
        edgeDF.show(false);
        GraphFrame graphFrame = new GraphFrame(pageVerDF, edgeDF);
        Dataset<Row> e = graphFrame
                .triplets()
                .groupBy("src", "dst")
                .agg(functions.count(functions.lit(1)).alias("weight"));
        e.repartition(32);
        e.show(false);
        e = e.select(col("src.keywords").alias("w1"), col("dst.keywords").alias("w2"), col("weight").alias("weight"));
        e.show(false);
        e = e.withColumn("word1", explode(col("w1")));
        e.show(false);
        e = e.withColumn("word2", explode(col("w2")))
                .select("word1", "word2", "weight");
        e.show(false);

        e = e.select(least("word1", "word2").as("src")
                , greatest("word1", "word2").alias("dst"), col("weight"));
        e.show(false);

        e.groupBy("src", "dst").agg(functions.sum("weight").alias("weight")).sort(desc("weight")).show(false);
//        List<Row> rows = new ArrayList<>();
//        rows.add(RowFactory.create(new String[]{"bomb", "war"}, new String[]{"daesh", "tank"}, 9));
//        rows.add(RowFactory.create(new String[]{"bomb", "daesh"}, new String[]{"war", "gun"}, 10));
//
//        StructType schema = new StructType(new StructField[]{
//                new StructField("src", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()),
//                new StructField("dst", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()),
//                new StructField("weight", DataTypes.IntegerType, false, Metadata.empty())
//        });
//        Dataset<Row> dataFrame = spark.createDataFrame(rows, schema);
//        dataFrame = dataFrame.withColumn("word1", explode(col("src")));
//        dataFrame = dataFrame.withColumn("word2", explode(col("dst")))
//                .select("word1", "word2", "weight");
//        dataFrame = dataFrame.select(least("word1", "word2").as("src")
//                , greatest("word1", "word2").alias("dst"), col("weight"));
//        dataFrame.show();
//
//        JavaRDD<Edge> rdd = dataFrame.toJavaRDD().map(row -> {
//            String src = row.getString(0);
//            String dst = row.getString(1);
//            if (src.compareTo(dst) > 0)
//                return new Edge(src, dst, row.getInt(2));
//            return new Edge(dst, src, row.getInt(2));
//        });
//        Dataset<Row> edgeDataset = spark.createDataFrame(rdd, Edge.class);
//        edgeDataset.show(false);

//        Dataset<Row> verticesPart1 = dataFrame.select(col("word1")).distinct();
//        Dataset<Row> verticesPart2 = dataFrame.select(col("word2")).distinct();
//        Dataset<Row> vertices = verticesPart1.union(verticesPart2).distinct()
//                .select(col("word1").as("id"));
//        vertices.show();
//        GraphFrame graphFrame = new GraphFrame(vertices,
//                dataFrame.select(col("word1").as("src"), col("word2").as("dst"), col("weight")));
//        graphFrame.triplets().show();
        spark.stop();
    }
}
