package in.nimbo;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.config.SiteGraphConfig;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.graphframes.GraphFrame;
import scala.Tuple2;

public class App {
    public static void main(String[] args) {
        SiteGraphConfig siteGraphConfig = SiteGraphConfig.load();
        HBaseConfig hBaseConfig = HBaseConfig.load();

        byte[] anchorColumnFamily = hBaseConfig.getAnchorColumnFamily();
        byte[] rankColumn = hBaseConfig.getRankColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, hBaseConfig.getLinksTable());
        hBaseConfiguration.set(TableInputFormat.SCAN_BATCHSIZE, siteGraphConfig.getScanBatchSize());

        SparkSession spark = SparkSession.builder()
                .appName(siteGraphConfig.getAppName())
                .getOrCreate();

        JavaRDD<Result> hBaseRDD = spark.sparkContext()
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                        , ImmutableBytesWritable.class, Result.class).toJavaRDD()
                .map(tuple -> tuple._2);

        JavaRDD<Node> nodes = hBaseRDD
                .map(result -> {
                    double rank = 0;
                    String rankString = Bytes.toString(result.getValue(rankColumn, rankColumn));
                    if (rankString != null)
                        rank = Double.parseDouble(rankString);
                    return new Node(getMainDomainForReversed(Bytes.toString(result.getRow())), rank);
                });

        JavaPairRDD<String, String> links = hBaseRDD
                .flatMapToPair(result -> result.getFamilyMap(anchorColumnFamily).entrySet().stream().map(
                        entry -> {
                            String anchorLink = Bytes.toString(entry.getKey());
                            int index = anchorLink.indexOf('#');
                            if (index != -1)
                                anchorLink = anchorLink.substring(0, index);
                            return new Tuple2<>(Bytes.toString(result.getRow()), anchorLink);
                        }).iterator());
        JavaPairRDD<String, String> mainDomains = links.mapToPair(link ->
                new Tuple2<>(getMainDomainForReversed(link._1), getMainDomain(link._2)));

        JavaPairRDD<String, String> filteredMainDomains = mainDomains.filter(domain -> !domain._1.equals(domain._2));

        JavaRDD<Edge> edges = filteredMainDomains.map(link ->
                new Edge(link._1, link._2));

        Dataset<Row> verDF = spark.createDataFrame(nodes, Node.class)
                .groupBy("id")
                .agg(functions.avg("rank"), functions.sum("numOfPages"));
        verDF.show(false);

        Dataset<Row> edgDF = spark.createDataFrame(edges, Edge.class);
        edgDF.show(false);

        GraphFrame graphFrame = new GraphFrame(verDF, edgDF);
        Dataset<Row> edgesWithWeight = graphFrame
                .triplets()
                .groupBy("src", "dst")
                .agg(functions.sum("edge.numOfAnchors"));
        edgesWithWeight.show(false);

        spark.stop();
    }

    private static String getMainDomain(String link) {
        String linkWithoutProtocol = link.substring(link.indexOf('/') + 2);
        int indexOfSlash = linkWithoutProtocol.indexOf('/');
        if (indexOfSlash != -1) {
            linkWithoutProtocol = linkWithoutProtocol.substring(0, indexOfSlash);
        }
        int lastDot = linkWithoutProtocol.lastIndexOf('.');
        int beforeLastDot = linkWithoutProtocol.substring(0, lastDot).lastIndexOf('.');
        return beforeLastDot == -1 ? linkWithoutProtocol : linkWithoutProtocol.substring(beforeLastDot + 1);
    }

    private static String getMainDomainForReversed(String link) {
        String linkWithoutProtocol = link.substring(link.indexOf('/') + 2);
        int indexOfSlash = linkWithoutProtocol.indexOf('/');
        if (indexOfSlash != -1) {
            linkWithoutProtocol = linkWithoutProtocol.substring(0, indexOfSlash);
        }
        int firstDot = linkWithoutProtocol.indexOf('.');
        int afterFirstDot = linkWithoutProtocol.indexOf('.', firstDot + 1);
        if (afterFirstDot != -1)
            linkWithoutProtocol = linkWithoutProtocol.substring(0, afterFirstDot + firstDot + 1);
        return linkWithoutProtocol.substring(firstDot + 1) + "." + linkWithoutProtocol.substring(0, firstDot);
    }
}
