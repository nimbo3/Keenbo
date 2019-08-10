package in.nimbo;

import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.AppConfig;
import in.nimbo.entity.Page;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;


public class App {
    public static void main(String[] args) {
        AppConfig appConfig = AppConfig.load();

        SparkConf sparkConf = new SparkConf()
                .setAppName(appConfig.getAppName())
                .setMaster(appConfig.getResourceManager())
                .set("es.nodes", appConfig.getNodesIP())
                .set("es.write.operation", "upsert")
                .set("es.mapping.id", "id")
                .set("es.index.auto.create", appConfig.getEsCreateIndex());

        SparkSession spark = SparkSession.builder()
                .appName(appConfig.getAppName())
                .config("spark.sql.warehouse.dir", "/file:C:/temp")
                .master("local[2]")
                .getOrCreate();

        String columnFamily = appConfig.getHbaseColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, appConfig.getHbaseTable());
        hBaseConfiguration.set(TableInputFormat.SCAN_BATCHSIZE, appConfig.getScanBatchSize());

        try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<Result> hBaseRDD = javaSparkContext
                    .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                            , ImmutableBytesWritable.class, Result.class).values();
            byte[] columnFamilyBytes = Bytes.toBytes(columnFamily);
            JavaPairRDD<String, String> a = hBaseRDD
                    .flatMap(result -> result.getFamilyMap(columnFamilyBytes).entrySet().iterator())
                    .mapToPair(entry -> new Tuple2<>(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue())));
            JavaPairRDD<String, Iterable<String>> reduced = a.groupByKey();

            JavaRDD<Page> pages = reduced.map(tuple2 -> new Page(LinkUtility.hashLink(tuple2._1), tuple2._2));

            JavaEsSpark.saveToEs(pages, appConfig.getEsIndexName() + "/" + appConfig.getEsTableName());
        }
    }
}
