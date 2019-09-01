package in.nimbo;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.utility.SparkUtility;
import in.nimbo.config.BackwardExtractorConfig;
import in.nimbo.entity.Page;
import in.nimbo.service.BackwardExtractorService;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public class App {
    public static void main(String[] args) {
        BackwardExtractorConfig backwardExtractorConfig = BackwardExtractorConfig.load();
        HBaseConfig hBasePageConfig = HBaseConfig.load();
        String esIndex = backwardExtractorConfig.getEsIndexName();
        String esType = backwardExtractorConfig.getEsType();

        SparkSession spark = loadSpark(backwardExtractorConfig.getAppName(), false);
        spark.sparkContext().conf().set("es.nodes", esIndex);
        spark.sparkContext().conf().set("es.write.operation", "upsert");
        spark.sparkContext().conf().set("es.mapping.id", "id");
        spark.sparkContext().conf().set("es.index.auto.create", "auto");

        JavaRDD<Result> hBaseRDD = SparkUtility.getHBaseRDD(spark, hBasePageConfig.getPageTable());
        hBaseRDD.persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<Page> extract = BackwardExtractorService.extractBackward(hBasePageConfig, spark, hBaseRDD);
        JavaEsSpark.saveToEs(extract, esIndex + "/" + esType);
        spark.stop();
    }

    public static SparkSession loadSpark(String appName, boolean isLocal) {
        SparkSession spark = SparkUtility.getSpark(appName, isLocal);
        SparkUtility.registerKryoClasses(spark, new Class[]{
                in.nimbo.common.entity.Anchor.class, in.nimbo.common.utility.SparkUtility.class, in.nimbo.common.config.RedisConfig.class,
                in.nimbo.common.entity.Meta.class, in.nimbo.common.entity.Page.class, in.nimbo.common.utility.LinkUtility.class,
                in.nimbo.common.exception.HBaseException.class, in.nimbo.common.config.KafkaConfig.class,
                in.nimbo.common.config.ElasticConfig.class, in.nimbo.common.serializer.PageDeserializer.class,
                in.nimbo.common.utility.CloseUtility.class, in.nimbo.common.exception.ElasticException.class,
                in.nimbo.common.exception.LoadConfigurationException.class, in.nimbo.common.exception.ReverseLinkException.class,
                HBaseConfig.class, in.nimbo.common.config.ProjectConfig.class,
                in.nimbo.common.monitoring.ThreadsMonitor.class, in.nimbo.common.serializer.PageSerializer.class,
                in.nimbo.common.entity.GraphResult.class, in.nimbo.common.exception.ParseLinkException.class,
                in.nimbo.common.exception.InvalidLinkException.class, in.nimbo.common.exception.LanguageDetectException.class,
                in.nimbo.common.exception.HashException.class, in.nimbo.common.config.Config.class,
                in.nimbo.entity.Edge.class, in.nimbo.entity.Page.class,
                in.nimbo.entity.Node.class, in.nimbo.App.class, in.nimbo.config.BackwardExtractorConfig.class,
                org.apache.hadoop.hbase.client.Result.class, org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,
                TableInputFormat.class, in.nimbo.common.entity.GraphResult.class});
        return spark;
    }
}
