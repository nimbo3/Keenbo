package in.nimbo.common.utility;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.io.IOException;
import java.util.Map;

public class SparkUtility {
    private SparkUtility() {
    }

    public static SparkSession getSpark(String appName, boolean isLocal) {
        System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkSession spark;
        if (isLocal) {
            spark = SparkSession.builder().appName(appName).master("local").getOrCreate();
        } else {
            spark = SparkSession.builder().appName(appName).getOrCreate();
        }
        spark.sparkContext().conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        spark.sparkContext().conf().set("spark.kryo.registrationRequired", "true");
        spark.sparkContext().conf().set("spark.speculation", "false");
        spark.sparkContext().conf().set("spark.hadoop.mapreduce.map.speculative", "false");
        spark.sparkContext().conf().set("spark.hadoop.mapreduce.reduce.speculative", "false");
        return spark;
    }

    public static void registerKryoClasses(SparkSession sparkSession, Class[] kyroClasses) {
        sparkSession.sparkContext().conf().registerKryoClasses(kyroClasses);
    }

    public static JavaRDD<Result> getHBaseRDD(SparkSession sparkSession, String tableName) {
        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.addResource(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml");
        hBaseConfiguration.addResource(System.getenv("HBASE_HOME") + "/conf/hbase-site.xml");
        hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, tableName);
        hBaseConfiguration.set(TableInputFormat.SCAN_BATCHSIZE, "500");

        return sparkSession.sparkContext()
                .newAPIHadoopRDD(hBaseConfiguration, TableInputFormat.class
                        , ImmutableBytesWritable.class, Result.class).toJavaRDD()
                .map(tuple -> tuple._2);
    }

    public static JavaPairRDD<String, Map<String, Object>> getElasticSearchRDD(SparkSession sparkSession, String indexName, String indexType) {
        JavaSparkContext javaSparkContext = getJavaSparkContext(sparkSession);
        return JavaEsSpark.esRDD(javaSparkContext, indexName + "/" + indexType);
    }

    public static JavaSparkContext getJavaSparkContext(SparkSession sparkSession) {
        return new JavaSparkContext(sparkSession.sparkContext());
    }

    public static JavaRDD<String> createJson(Dataset<Row> dataset) {
        long count = dataset.count();
        return dataset.toJSON().repartition(1)
                .javaRDD()
                .zipWithIndex()
                .map(val -> {
                    if (val._2 == 0) {
                        return "[\n" + val._1 + ",";
                    } else {
                        return val._2 == count - 1 ? val._1 + "\n]" : val._1 + ",";
                    }
                });
    }

    public static void saveToHBase(String tableName, JavaPairRDD<ImmutableBytesWritable, Put> put) {
        try {
            Job jobConf = Job.getInstance();
            jobConf.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
            jobConf.setOutputFormatClass(TableOutputFormat.class);
            jobConf.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
            put.saveAsNewAPIHadoopDataset(jobConf.getConfiguration());
        } catch (IOException e) {
            System.out.println("Unable to save to HBase");
            e.printStackTrace(System.out);
        }
    }
}
