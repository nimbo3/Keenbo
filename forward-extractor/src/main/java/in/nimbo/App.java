package in.nimbo;

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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.NavigableMap;


public class App {
    public static void main(String[] args) {
        AppConfig appConfig = AppConfig.load();

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(appConfig.getAppName());
        sparkConf.setMaster(appConfig.getResourceManager());

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        System.out.println("Configuring hBaseConfiguration"); // TODO use logger
        Configuration hBaseConfiguration;
        hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource("$HADOOP_HOME/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource("$HBASE_HOME/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, appConfig.getTableName());
        hBaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, appConfig.getColumnFamily());

        JavaRDD<Result> hBaseRDD = javaSparkContext
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                        , ImmutableBytesWritable.class, Result.class).values();
        String columnFamily = appConfig.getColumnFamily();

        JavaPairRDD<String, ArrayList<String>> map = hBaseRDD.flatMapToPair((PairFlatMapFunction<Result, String, ArrayList<String>>) row -> {
            ArrayList<Tuple2<String, ArrayList<String>>> result = new ArrayList<>();
            NavigableMap<byte[], byte[]> familyMap = row.getFamilyMap(Bytes.toBytes(columnFamily));
            familyMap.forEach((qualifier, value) -> {
                String link = Bytes.toString(qualifier);
                String text = Bytes.toString(value);
                result.add(new Tuple2<>(link, new ArrayList<>(Collections.singletonList(text))));
            });
            return result.iterator();
        });

        JavaPairRDD<String, ArrayList<String>> reduced = map.reduceByKey((v1, v2) -> {
            if (v1.size() > v2.size()) {
                v1.addAll(v2);
                return v1;
            } else {
                v2.addAll(v1);
                return v2;
            }
        });

        String time = LocalDateTime.now().toString();
        reduced.saveAsTextFile("output/" + time);
    }
}
