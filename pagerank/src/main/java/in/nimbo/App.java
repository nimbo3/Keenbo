package in.nimbo;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.PageRankConfig;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;
import in.nimbo.entity.Page;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.graphframes.GraphFrame;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public class App {
    public static void main(String[] args) {
        HBaseConfig hBaseConfig = HBaseConfig.load();
        PageRankConfig pageRankConfig = PageRankConfig.load();
        String esIndex = pageRankConfig.getEsIndex();
        String esType = pageRankConfig.getEsType();
        byte[] rankColumn = hBaseConfig.getDataColumnFamily();
        byte[] anchorColumnFamily = hBaseConfig.getAnchorColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, hBaseConfig.getLinksTable());

        SparkSession spark = SparkSession.builder()
                .config("spark.hadoop.validateOutputSpecs", false)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryoserializer.buffer", "1024k")
                .appName(pageRankConfig.getAppName())
                .getOrCreate();

        spark.sparkContext().conf().registerKryoClasses(new Class[]{
                in.nimbo.common.utility.LinkUtility.class, in.nimbo.common.exception.ParseLinkException.class, in.nimbo.common.entity.Anchor.class,
                in.nimbo.common.config.Config.class, in.nimbo.common.config.HBaseConfig.class, in.nimbo.common.utility.CloseUtility.class,
                in.nimbo.common.config.RedisConfig.class, in.nimbo.common.config.ElasticConfig.class, in.nimbo.common.entity.Page.class,
                in.nimbo.common.exception.ElasticException.class, in.nimbo.common.serializer.PageDeserializer.class,
                in.nimbo.common.exception.InvalidLinkException.class, in.nimbo.common.serializer.PageSerializer.class,
                in.nimbo.common.entity.Meta.class, in.nimbo.common.exception.LoadConfigurationException.class,
                in.nimbo.common.exception.LanguageDetectException.class, in.nimbo.common.exception.ReverseLinkException.class,
                in.nimbo.common.exception.HBaseException.class, in.nimbo.common.exception.HashException.class,
                in.nimbo.common.config.KafkaConfig.class, in.nimbo.common.config.ProjectConfig.class, in.nimbo.entity.Edge.class,
                in.nimbo.entity.Page.class, in.nimbo.entity.Node.class, in.nimbo.App.class, in.nimbo.config.PageRankConfig.class
        });

        spark.sparkContext().conf().set("spark.speculation", "false");
        spark.sparkContext().conf().set("spark.hadoop.mapreduce.map.speculative", "false");
        spark.sparkContext().conf().set("spark.hadoop.mapreduce.reduce.speculative", "false");
        spark.sparkContext().conf().set("es.nodes", pageRankConfig.getEsNodes());
        spark.sparkContext().conf().set("es.write.operation", pageRankConfig.getEsWriteOperation());
        spark.sparkContext().conf().set("es.mapping.id", "id");
        spark.sparkContext().conf().set("es.index.auto.create", pageRankConfig.getEsIndexAutoCreate());
        spark.sparkContext().conf().set("spark.kryo.registrationRequired", "true");

        JavaRDD<Result> hBaseRDD = spark.sparkContext()
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class).toJavaRDD()
                .map(tuple -> tuple._2);
        hBaseRDD.persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<Node> nodes = hBaseRDD.map(result -> new Node(Bytes.toString(result.getRow())));
        JavaRDD<Edge> edges = hBaseRDD.flatMap(result -> result.listCells().iterator())
                .filter(cell -> CellUtil.matchingFamily(cell, anchorColumnFamily))
                .map(cell -> new Edge(
                        Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()),
                        LinkUtility.reverseLink(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()))
                ));
        hBaseRDD.unpersist();

        hBaseRDD.intersection(hBaseRDD);
        Dataset<Row> vertexDF = spark.createDataFrame(nodes, Node.class);
        Dataset<Row> edgeDF = spark.createDataFrame(edges, Edge.class);
        edgeDF.repartition(32);
        GraphFrame graphFrame = new GraphFrame(vertexDF, edgeDF);
        GraphFrame pageRank = graphFrame.pageRank().maxIter(pageRankConfig.getMaxIter()).resetProbability(pageRankConfig.getResetProbability()).run();
        JavaRDD<Row> pageRankRdd = pageRank.vertices().toJavaRDD();
        pageRankRdd.persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<ImmutableBytesWritable, Put> javaPairRDD = pageRankRdd.mapToPair(row -> {
            Put put = new Put(Bytes.toBytes(row.getString(0)));
            put.addColumn(rankColumn, rankColumn, Bytes.toBytes(String.valueOf(row.getDouble(1))));
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });

        JavaRDD<Page> esPageJavaRDD = pageRankRdd
                .map(row -> new Page(
                        LinkUtility.hashLink(LinkUtility.reverseLink(row.getString(0))),
                        row.getDouble(1)));

        pageRankRdd.unpersist();

        try {
            Job jobConf = Job.getInstance();
            jobConf.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hBaseConfig.getLinksTable());
            jobConf.setOutputFormatClass(TableOutputFormat.class);
            jobConf.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
            javaPairRDD.saveAsNewAPIHadoopDataset(jobConf.getConfiguration());
        } catch (IOException e) {
            System.out.println("Unable to save to HBase");
            e.printStackTrace(System.out);
        }

        JavaEsSpark.saveToEs(esPageJavaRDD, esIndex + "/" + esType);

        spark.stop();
    }
}
