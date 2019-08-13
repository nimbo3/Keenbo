package in.nimbo;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.config.AppConfig;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.graphframes.GraphFrame;

public class App {
    public static void main(String[] args) {
        AppConfig appConfig = AppConfig.load();
        HBaseConfig hBaseConfig = HBaseConfig.load();

        byte[] anchorColumnFamily = hBaseConfig.getAnchorColumnFamily();
        byte[] rankColumn = hBaseConfig.getRankColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, hBaseConfig.getLinksTable());
        hBaseConfiguration.set(TableInputFormat.SCAN_BATCHSIZE, appConfig.getScanBatchSize());

        SparkSession spark = SparkSession.builder()
                .appName(appConfig.getAppName())
                .master(appConfig.getResourceManager())
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

        JavaRDD<Edge> edges = hBaseRDD
                .flatMap(result -> result.getFamilyMap(anchorColumnFamily).entrySet().stream().map(
                        entry -> {
                            String anchorLink = Bytes.toString(entry.getKey());
                            int index = anchorLink.indexOf("#");
                            if (index != -1)
                                anchorLink = anchorLink.substring(0, index);
                            anchorLink = getMainDomain(anchorLink);
                            return new Edge(getMainDomainForReversed(Bytes.toString(result.getRow())), anchorLink);
                        }).iterator());

        Dataset<Row> verDF = spark.createDataFrame(nodes, Node.class);

        Dataset<Row> edgDF = spark.createDataFrame(edges, Edge.class);

        GraphFrame graphFrame = new GraphFrame(verDF, edgDF);
        Dataset<Row> edgesWithWeight = graphFrame.triplets().groupBy("src", "dst")
                .agg(functions.sum("edge.numOfAnchors"));

        edgesWithWeight.show();
//        JavaRDD<Page> anchorsRDD = anchors.toJavaRDD()
//                .map(row -> new Page(
//                        LinkUtility.hashLink(LinkUtility.reverseLink(row.getStruct(0).getString(0))),
//                        row.getList(1), row.getLong(2)));

//        JavaEsSpark.saveToEs(anchorsRDD, appConfig.getEsIndexName() + "/" + appConfig.getEsType());

        spark.stop();
    }

    private static String getMainDomain(String link) {
//        String linkWithoutProtocol = link.substring(link.indexOf('/') + 2);
//        int indexOfSlash = linkWithoutProtocol.indexOf('/');
//        if (indexOfSlash != -1) {
//            linkWithoutProtocol = linkWithoutProtocol.substring(0, indexOfSlash);
//        }
        String linkWithoutProtocol = link;
        int lastDot = linkWithoutProtocol.lastIndexOf('.');
        int beforeLastDot = linkWithoutProtocol.substring(0, lastDot).lastIndexOf('.');
        return beforeLastDot == -1 ? linkWithoutProtocol : linkWithoutProtocol.substring(beforeLastDot + 1);
    }

    private static String getMainDomainForReversed(String link) {
//        String linkWithoutProtocol = link.substring(link.indexOf('/') + 2);
//        int indexOfSlash = linkWithoutProtocol.indexOf('/');
//        if (indexOfSlash != -1) {
//            linkWithoutProtocol = linkWithoutProtocol.substring(0, indexOfSlash);
//        }
        String linkWithoutProtocol = link;
        int firstDot = linkWithoutProtocol.indexOf('.');
        int afterFirstDot = linkWithoutProtocol.substring(firstDot + 1).indexOf('.');
        if (afterFirstDot != -1)
            linkWithoutProtocol = linkWithoutProtocol.substring(0, afterFirstDot + firstDot + 1);
        return linkWithoutProtocol.substring(firstDot + 1) + "." + linkWithoutProtocol.substring(0, firstDot);
    }
}
