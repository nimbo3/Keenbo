package in.nimbo;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.BackwardExtractorConfig;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;
import in.nimbo.entity.Page;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.graphframes.GraphFrame;

import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.count;


public class App {
    public static void main(String[] args) {
        BackwardExtractorConfig backwardExtractorConfig = BackwardExtractorConfig.load();
        HBaseConfig hBaseConfig = HBaseConfig.load();

        byte[] anchorColumnFamily = hBaseConfig.getAnchorColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, hBaseConfig.getLinksTable());
        hBaseConfiguration.set(TableInputFormat.SCAN_BATCHSIZE, backwardExtractorConfig.getScanBatchSize());

        SparkSession spark = SparkSession.builder()
                .appName(backwardExtractorConfig.getAppName())
                .getOrCreate();
        spark.sparkContext().conf().set("es.nodes", backwardExtractorConfig.getNodesIP());
        spark.sparkContext().conf().set("es.write.operation", "upsert");
        spark.sparkContext().conf().set("es.mapping.id", "id");
        spark.sparkContext().conf().set("es.index.auto.create", backwardExtractorConfig.getEsCreateIndex());

        JavaRDD<Result> hBaseRDD = spark.sparkContext()
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                        , ImmutableBytesWritable.class, Result.class).toJavaRDD()
                .map(tuple -> tuple._2);

        JavaRDD<Node> nodes = hBaseRDD
                .map(result -> new Node(Bytes.toString(result.getRow())));

        JavaRDD<Edge> edges = hBaseRDD.flatMap(result -> result.listCells().iterator())
                .filter(cell -> CellUtil.matchingFamily(cell, anchorColumnFamily))
                .map(cell -> {
                    String rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    String anchorLink = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    int index = anchorLink.indexOf('#');
                    if (index != -1) {
                        anchorLink = anchorLink.substring(0, index);
                    }
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    return new Edge(rowKey, LinkUtility.reverseLink(anchorLink), value);
                });

        Dataset<Row> verDF = spark.createDataFrame(nodes, Node.class);

        Dataset<Row> edgDF = spark.createDataFrame(edges, Edge.class);

        GraphFrame graphFrame = new GraphFrame(verDF, edgDF);
        Dataset<Row> anchors = graphFrame.triplets().groupBy("dst")
                .agg(collect_set("edge.anchor").alias("anchors"), count("edge.anchor").alias("count"));

        JavaRDD<Page> anchorsRDD = anchors.toJavaRDD()
                .map(row -> new Page(
                        LinkUtility.hashLink(LinkUtility.reverseLink(row.getStruct(0).getString(0))),
                        row.getList(1), row.getLong(2)));

        JavaEsSpark.saveToEs(anchorsRDD, backwardExtractorConfig.getEsIndexName() + "/" + backwardExtractorConfig.getEsType());

        spark.stop();
    }
}
