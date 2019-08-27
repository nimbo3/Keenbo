package in.nimbo;

import in.nimbo.common.config.HBasePageConfig;
import in.nimbo.common.config.HBaseSiteConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.SiteGraphConfig;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;
import in.nimbo.service.GraphExtractor;
import in.nimbo.service.SiteExtractor;
import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args) {
        SiteGraphConfig siteGraphConfig = SiteGraphConfig.load();
        HBaseSiteConfig hBaseSiteConfig = HBaseSiteConfig.load();
        HBasePageConfig hBasePageConfig = HBasePageConfig.load();

        SparkSession spark = SparkSession.builder()
                .appName(siteGraphConfig.getAppName())
                .getOrCreate();
        spark.sparkContext().conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        spark.sparkContext().conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        spark.sparkContext().conf().set("spark.kryo.registrationRequired", "true");
        spark.sparkContext().conf().set("spark.speculation", "false");
        spark.sparkContext().conf().set("spark.hadoop.mapreduce.map.speculative", "false");
        spark.sparkContext().conf().set("spark.hadoop.mapreduce.reduce.speculative", "false");

        spark.sparkContext().conf().registerKryoClasses(new Class[]{in.nimbo.common.entity.Meta.class,
                in.nimbo.common.exception.LoadConfigurationException.class, HBasePageConfig.class,
                in.nimbo.common.config.ElasticConfig.class, in.nimbo.common.entity.Anchor.class,
                in.nimbo.common.exception.LanguageDetectException.class, in.nimbo.common.config.ProjectConfig.class,
                in.nimbo.common.utility.CloseUtility.class, in.nimbo.common.exception.HBaseException.class,
                LinkUtility.class, in.nimbo.common.config.KafkaConfig.class,
                in.nimbo.common.entity.Page.class, in.nimbo.common.exception.ReverseLinkException.class,
                in.nimbo.common.exception.ElasticException.class, in.nimbo.common.exception.ParseLinkException.class,
                in.nimbo.common.serializer.PageSerializer.class, in.nimbo.common.exception.HashException.class,
                in.nimbo.common.config.Config.class, in.nimbo.common.config.RedisConfig.class,
                in.nimbo.common.serializer.PageDeserializer.class, in.nimbo.common.exception.InvalidLinkException.class,
                Edge.class, Node.class, in.nimbo.App.class, SiteGraphConfig.class});

        if (siteGraphConfig.isExtractor()) {
            SiteExtractor.extract(hBasePageConfig, hBaseSiteConfig, siteGraphConfig, spark);
        } else if (siteGraphConfig.isGraph()) {
            GraphExtractor.extract(hBaseSiteConfig, siteGraphConfig, spark);
        }
    }
}
