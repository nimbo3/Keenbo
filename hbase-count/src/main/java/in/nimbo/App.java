package in.nimbo;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.config.HBaseCountConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args) {
        HBaseCountConfig hBaseCountConfig = HBaseCountConfig.load();
        HBaseConfig hBaseConfig = HBaseConfig.load();

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, hBaseConfig.getLinksTable());
        hBaseConfiguration.set(TableInputFormat.SCAN_BATCHSIZE, hBaseCountConfig.getScanBatchSize());


        SparkSession spark = SparkSession.builder()
                .appName(hBaseCountConfig.getAppName())
                .getOrCreate();

        spark.sparkContext().conf().set("spark.driver.extraClassPath", "local:/var/local/target/lib/*");
        spark.sparkContext().conf().set("spark.executor.extraClassPath", "local:/var/local/target/lib/*");

        long hBaseRDDCount = spark.sparkContext()
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                        , ImmutableBytesWritable.class, Result.class).toJavaRDD()
                .count();

        System.out.println(hBaseRDDCount);

        spark.stop();
    }
}
