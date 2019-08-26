package in.nimbo;

import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.config.HBaseSiteConfig;
import in.nimbo.config.SiteGraphConfig;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;
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
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;
import scala.Tuple2;

import java.io.IOException;
import java.util.Objects;

public class App {
    public static void main(String[] args) {
        SiteGraphConfig siteGraphConfig = SiteGraphConfig.load();
        HBaseSiteConfig hBaseSiteConfig = HBaseSiteConfig.load();
        HBasePageConfig hBasePageConfig = HBasePageConfig.load();

        String siteTable = hBaseSiteConfig.getSiteTable();
        byte[] infoColumnFamily = hBaseSiteConfig.getInfoColumnFamily();
        byte[] domainColumnFamily = hBaseSiteConfig.getDomainColumnFamily();
        byte[] countColumn = hBaseSiteConfig.getCountColumn();
        byte[] siteRankColumn = hBaseSiteConfig.getRankColumn();

        byte[] rankColumnFamily = hBasePageConfig.getRankColumnFamily();
        byte[] pageRankColumn = hBasePageConfig.getRankColumn();
        byte[] anchorColumnFamily = hBasePageConfig.getAnchorColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, hBasePageConfig.getPageTable());
        hBaseConfiguration.set(TableInputFormat.SCAN_BATCHSIZE, siteGraphConfig.getScanBatchSize());

        SparkSession spark = SparkSession.builder()
                .appName(siteGraphConfig.getAppName())
                .getOrCreate();
        spark.sparkContext().conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        spark.sparkContext().conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        spark.sparkContext().conf().set("spark.kryo.registrationRequired", "true");
        spark.sparkContext().conf().set("spark.speculation", "false");
        spark.sparkContext().conf().set("spark.hadoop.mapreduce.map.speculative", "false");
        spark.sparkContext().conf().set("spark.hadoop.mapreduce.reduce.speculative", "false");

        spark.sparkContext().conf().registerKryoClasses(new Class[]{in.nimbo.common.entity.Meta.class,
                in.nimbo.common.exception.LoadConfigurationException.class, HBasePageConfig.class,
                in.nimbo.common.config.ElasticConfig.class, in.nimbo.common.entity.Anchor.class,
                in.nimbo.common.exception.LanguageDetectException.class, in.nimbo.common.config.ProjectConfig.class,
                in.nimbo.common.utility.CloseUtility.class, in.nimbo.common.exception.HBaseException.class,
                in.nimbo.common.utility.LinkUtility.class, in.nimbo.common.config.KafkaConfig.class,
                in.nimbo.common.entity.Page.class, in.nimbo.common.exception.ReverseLinkException.class,
                in.nimbo.common.exception.ElasticException.class, in.nimbo.common.exception.ParseLinkException.class,
                in.nimbo.common.serializer.PageSerializer.class, in.nimbo.common.exception.HashException.class,
                in.nimbo.common.config.Config.class, in.nimbo.common.config.RedisConfig.class,
                in.nimbo.common.serializer.PageDeserializer.class, in.nimbo.common.exception.InvalidLinkException.class,
                in.nimbo.entity.Edge.class, in.nimbo.entity.Node.class, in.nimbo.App.class, in.nimbo.config.SiteGraphConfig.class});

        JavaRDD<Result> hBaseRDD = spark.sparkContext()
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                        , ImmutableBytesWritable.class, Result.class).toJavaRDD()
                .map(tuple -> tuple._2);
        hBaseRDD.persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<Node> nodes = hBaseRDD
                .map(result -> result.getColumnLatestCell(rankColumnFamily, pageRankColumn))
                .filter(Objects::nonNull)
                .map(cell -> {
                    String rankStr = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    return new Node(
                            getMainDomainForReversed(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                            ),
                            Double.parseDouble(rankStr)
                    );
                });

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
                }).mapToPair(link -> new Tuple2<>(getMainDomainForReversed(link._1), getMainDomain(link._2))).
                filter(domain -> !domain._1.equals(domain._2)).
                map(link -> new Edge(link._1, link._2));

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

        try {
            Job jobConf = Job.getInstance();
            jobConf.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, siteTable);
            jobConf.setOutputFormatClass(TableOutputFormat.class);
            jobConf.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
            verticesPut.saveAsNewAPIHadoopDataset(jobConf.getConfiguration());

            jobConf = Job.getInstance();
            jobConf.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, siteTable);
            jobConf.setOutputFormatClass(TableOutputFormat.class);
            jobConf.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
            edgesSCCPut.saveAsNewAPIHadoopDataset(jobConf.getConfiguration());
        } catch (IOException e) {
            System.out.println("Unable to save to HBase");
            e.printStackTrace(System.out);
        }

        spark.stop();
    }

    private static String getMainDomain(String link) {
        try {
            String domain = getDomain(link);
            int lastDot = domain.lastIndexOf('.');
            int beforeLastDot = domain.substring(0, lastDot).lastIndexOf('.');
            return beforeLastDot == -1 ? domain : domain.substring(beforeLastDot + 1);
        } catch (Throwable e) {
            e.printStackTrace();
            return "@@@";
        }
    }

    private static String getMainDomainForReversed(String link) {
        try {
            String domain = getDomain(link);
            int firstDot = domain.indexOf('.');
            int afterFirstDot = domain.indexOf('.', firstDot + 1);
            if (afterFirstDot != -1) {
                domain = domain.substring(0, afterFirstDot);
            }
            return domain.substring(firstDot + 1) + "." + domain.substring(0, firstDot);
        } catch (Throwable e) {
            e.printStackTrace();
            return "@@@";
        }
    }

    private static String getDomain(String link) {
        try {
            int indexOfProtocol = link.indexOf('/') + 1;
            int indexOfEndDomain = link.indexOf('/', indexOfProtocol + 1);
            if (indexOfEndDomain < 0) {
                indexOfEndDomain = link.length();
            }
            String domain = link.substring(indexOfProtocol + 1, indexOfEndDomain);
            int colonIndex = domain.indexOf(':');
            if (colonIndex > -1) {
                domain = domain.substring(0, colonIndex);
            }
            return domain;
        } catch (Throwable e) {
            e.printStackTrace();
            return "@@@";
        }
    }
}
