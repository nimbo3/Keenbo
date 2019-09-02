package in.nimbo;

import in.nimbo.common.config.ProjectConfig;
import in.nimbo.common.utility.SparkUtility;
import in.nimbo.config.ClassifierConfig;
import in.nimbo.service.ModelExtractorService;
import in.nimbo.service.ModelInfo;
import in.nimbo.service.TrainingService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class App {
    public static void main(String[] args) {
        ProjectConfig projectConfig = ProjectConfig.load();
        ModelInfo modelInfo = new ModelInfo();

        ClassifierConfig classifierConfig = ClassifierConfig.load();
        if (classifierConfig.getAppMode() == ClassifierConfig.MODE.CRAWL) {
            runCrawler(classifierConfig, projectConfig);
        } else if (classifierConfig.getAppMode() == ClassifierConfig.MODE.CLASSIFY) {
            runExtractModel(classifierConfig, modelInfo);
        }
    }

    private static void runCrawler(ClassifierConfig classifierConfig, ProjectConfig projectConfig) {
        TrainingService trainingService = new TrainingService();
    }

    private static void runExtractModel(ClassifierConfig classifierConfig, ModelInfo modelInfo) {
        SparkSession spark = SparkUtility.getSpark(classifierConfig.getAppName(), true);
        spark.sparkContext().conf().set("es.nodes", classifierConfig.getEsNodes());
        spark.sparkContext().conf().set("es.write.operation", classifierConfig.getEsWriteOperation());
        spark.sparkContext().conf().set("es.mapping.id", "id");
        spark.sparkContext().conf().set("es.index.auto.create", classifierConfig.getEsIndexAutoCreate());
        JavaPairRDD<String, Map<String, Object>> elasticSearchRDD =
                SparkUtility.getElasticSearchRDD(spark, classifierConfig.getEsIndex(), classifierConfig.getEsType());
        ModelExtractorService.extractModel(classifierConfig, modelInfo, spark, elasticSearchRDD);
        spark.stop();
    }
}
