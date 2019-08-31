package in.nimbo;

import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.entity.GraphResult;
import in.nimbo.common.utility.SparkUtility;
import in.nimbo.config.WordGraphConfig;
import in.nimbo.service.WordGraphExtractorService;
import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args) {
        HBasePageConfig hBaseConfig = HBasePageConfig.load();
        WordGraphConfig wordGraphConfig = WordGraphConfig.load();

        SparkSession spark = loadSpark(wordGraphConfig.getAppName(), false);

        JavaRDD<Result> hBaseRDD = SparkUtility.getHBaseRDD(spark, hBaseConfig.getPageTable());
        GraphResult graphResult = WordGraphExtractorService.extract(hBaseConfig, spark, hBaseRDD);
        JavaRDD<String> nodesJson = SparkUtility.createJson(graphResult.getNodes());
        JavaRDD<String> edgesJson = SparkUtility.createJson(graphResult.getEdges());
        edgesJson.saveAsTextFile("/WordGraphEdges");
        nodesJson.saveAsTextFile("/WordGraphVertices");

        spark.stop();
    }

    public static SparkSession loadSpark(String appName, boolean isLocal) {
        SparkSession spark = SparkUtility.getSpark(appName, isLocal);
        SparkUtility.registerKryoClasses(spark, new Class[]{
                in.nimbo.common.monitoring.ThreadsMonitor.class, in.nimbo.common.entity.Page.class,
                in.nimbo.common.entity.Anchor.class, in.nimbo.common.entity.Meta.class,
                in.nimbo.common.utility.LinkUtility.class, in.nimbo.common.utility.CloseUtility.class,
                in.nimbo.common.exception.HBaseException.class, in.nimbo.common.exception.InvalidLinkException.class,
                in.nimbo.common.exception.HashException.class, in.nimbo.common.exception.LoadConfigurationException.class,
                in.nimbo.common.exception.ElasticException.class, in.nimbo.common.exception.LanguageDetectException.class,
                in.nimbo.common.exception.ParseLinkException.class, in.nimbo.common.exception.ReverseLinkException.class,
                in.nimbo.common.serializer.PageDeserializer.class, in.nimbo.common.serializer.PageSerializer.class,
                in.nimbo.common.config.RedisConfig.class, in.nimbo.common.config.KafkaConfig.class,
                in.nimbo.common.config.ElasticConfig.class, in.nimbo.common.config.ProjectConfig.class,
                in.nimbo.common.config.Config.class, in.nimbo.common.config.HBaseSiteConfig.class,
                in.nimbo.common.config.HBasePageConfig.class, in.nimbo.App.class, in.nimbo.config.WordGraphConfig.class

        });
        return spark;
    }
}
