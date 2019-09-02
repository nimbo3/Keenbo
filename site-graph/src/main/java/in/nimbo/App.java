package in.nimbo;

import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.common.utility.SparkUtility;
import in.nimbo.config.SiteGraphConfig;
import in.nimbo.entity.Edge;
import in.nimbo.entity.Node;
import in.nimbo.service.GraphExtractor;
import in.nimbo.common.entity.GraphResult;
import in.nimbo.service.SiteExtractor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

public class App {
    public static void main(String[] args) {
        SiteGraphConfig siteGraphConfig = SiteGraphConfig.load();
        HBaseConfig hBaseConfig = HBaseConfig.load();

        SparkSession spark = loadSpark(siteGraphConfig.getAppName(), false);

        if (siteGraphConfig.getAppMode() == SiteGraphConfig.MODE.EXTRACTOR) {
            JavaRDD<Result> hBaseRDD = SparkUtility.getHBaseRDD(spark, hBaseConfig.getPageTable());
            hBaseRDD.persist(StorageLevel.MEMORY_AND_DISK());
            Tuple2<JavaPairRDD<ImmutableBytesWritable, Put>, JavaPairRDD<ImmutableBytesWritable, Put>> extract =
                    SiteExtractor.extract(hBaseConfig, spark, hBaseRDD);
            SparkUtility.saveToHBase(hBaseConfig.getSiteTable(), extract._1);
            SparkUtility.saveToHBase(hBaseConfig.getSiteTable(), extract._2);
        } else if (siteGraphConfig.getAppMode() == SiteGraphConfig.MODE.GRAPH) {
            JavaRDD<Result> hBaseRDD = SparkUtility.getHBaseRDD(spark, hBaseConfig.getSiteTable());
            hBaseRDD.persist(StorageLevel.MEMORY_AND_DISK());
            GraphResult graphResult = GraphExtractor.extract(hBaseConfig, spark, hBaseRDD);
            JavaRDD<String> nodesJson = SparkUtility.createJson(graphResult.getNodes());
            JavaRDD<String> edgesJson = SparkUtility.createJson(graphResult.getEdges());
            nodesJson.saveAsTextFile("/SiteGraphVertices");
            edgesJson.saveAsTextFile("/SiteGraphEdges");
        }
        spark.stop();
    }

    public static SparkSession loadSpark(String appName, boolean isLocal) {
        SparkSession spark = SparkUtility.getSpark(appName, isLocal);
        SparkUtility.registerKryoClasses(spark, new Class[]{in.nimbo.common.entity.Meta.class,
                in.nimbo.common.exception.LoadConfigurationException.class, HBaseConfig.class,
                in.nimbo.common.config.ElasticConfig.class, in.nimbo.common.entity.Anchor.class,
                in.nimbo.common.exception.LanguageDetectException.class, in.nimbo.common.config.ProjectConfig.class,
                in.nimbo.common.utility.CloseUtility.class, in.nimbo.common.exception.HBaseException.class,
                LinkUtility.class, in.nimbo.common.config.KafkaConfig.class,
                in.nimbo.common.entity.Page.class, in.nimbo.common.exception.ReverseLinkException.class,
                in.nimbo.common.exception.ElasticException.class, in.nimbo.common.exception.ParseLinkException.class,
                in.nimbo.common.serializer.PageSerializer.class, in.nimbo.common.exception.HashException.class,
                in.nimbo.common.config.Config.class, in.nimbo.common.config.RedisConfig.class,
                in.nimbo.common.serializer.PageDeserializer.class, in.nimbo.common.exception.InvalidLinkException.class,
                Edge.class, Node.class, in.nimbo.App.class, SiteGraphConfig.class,
                org.apache.hadoop.hbase.client.Result.class, org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,
                TableInputFormat.class, in.nimbo.common.entity.GraphResult.class});
        return spark;
    }
}
