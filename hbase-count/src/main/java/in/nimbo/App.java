package in.nimbo;

import in.nimbo.common.utility.SparkUtility;
import in.nimbo.config.HBaseCountConfig;
import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.io.PrintStream;

public class App {
    public static void main(String[] args) throws FileNotFoundException {
        HBaseCountConfig hBaseCountConfig = HBaseCountConfig.load();
        SparkSession spark = SparkUtility.getSpark(hBaseCountConfig.getAppName(), true);
        JavaRDD<Result> hBaseRDD = SparkUtility.getHBaseRDD(spark, hBaseCountConfig.getTableName());
        System.setOut(new PrintStream("a.txt"));
        System.out.println("count " + hBaseRDD.count());
        spark.close();
    }
}
