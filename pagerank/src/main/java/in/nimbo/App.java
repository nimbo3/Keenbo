package in.nimbo;

import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.AppConfig;
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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Stream;

public class App {
    public static void main(String[] args) {
        AppConfig appConfig = AppConfig.load();
        SparkConf sparkConf = new SparkConf()
                .setAppName(appConfig.getAppName());

        String columnFamily = appConfig.getHbaseColumnFamily();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, appConfig.getHbaseTable());
        hBaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, columnFamily);

        try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<Result> hBaseRDD = javaSparkContext
                    .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                            , ImmutableBytesWritable.class, Result.class).values();

            JavaPairRDD<Set<byte[]>, Double> map = hBaseRDD
                    .mapToPair(result -> {
                        double rank = Bytes.toDouble(result.getValue(Bytes.toBytes("R"), Bytes.toBytes("R")));
                        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("A"));
                        int count = familyMap.keySet().size();
                        rank /= count;
                        return new Tuple2<>(familyMap.keySet(), rank);
                    });

            JavaPairRDD<byte[], Double> flatMapToPair = map.flatMapToPair(setRank -> setRank._1.stream().map(bytes ->
                    new Tuple2<>(bytes, setRank._2)).iterator());

            JavaPairRDD<byte[], Double> reduced = flatMapToPair.reduceByKey((v1, v2) -> (v1 + v2));
            reduced.saveAsTextFile("result.txt");
            /*JavaRDD<String> vertexes = hBaseRDD.map(result -> Bytes.toString(result.getRow()));
            JavaPairRDD<String, String> edges = hBaseRDD.flatMapToPair(result -> result.getFamilyMap(Bytes.toBytes("A")).keySet().stream().map(bytes -> {
                return new Tuple2<>(Bytes.toString(result.getRow()), Bytes.toString(bytes));
            }).iterator());*/
        }
    }
}
