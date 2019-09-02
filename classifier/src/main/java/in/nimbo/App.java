package in.nimbo;

import in.nimbo.common.config.ProjectConfig;
import in.nimbo.common.utility.SparkUtility;
import in.nimbo.config.ClassifierConfig;
import in.nimbo.service.ClassifierService;
import in.nimbo.service.ModelExtractorService;
import in.nimbo.service.ModelInfo;
import in.nimbo.service.TrainingService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

public class App {
    private static ClassifierConfig classifierConfig;
    public static void main(String[] args) {
        ProjectConfig projectConfig = ProjectConfig.load();
        ModelInfo modelInfo = new ModelInfo();
        classifierConfig = ClassifierConfig.load();

        if (classifierConfig.getAppMode() == ClassifierConfig.MODE.CRAWL) {
            runCrawler(projectConfig);
        } else if (classifierConfig.getAppMode() == ClassifierConfig.MODE.TRAIN) {
            runExtractModel(modelInfo);
        } else if (classifierConfig.getAppMode() == ClassifierConfig.MODE.CLASSIFY) {
            runClassifier(modelInfo);
        }
    }

    private static void runCrawler(ProjectConfig projectConfig) {
        TrainingService trainingService = new TrainingService();
    }

    private static void runExtractModel(ModelInfo modelInfo) {
        SparkSession spark = setSparkEsConfigs();
        JavaPairRDD<String, Map<String, Object>> elasticSearchRDD =
                SparkUtility.getElasticSearchRDD(spark, classifierConfig.getEsIndex(), classifierConfig.getEsType());
        ModelExtractorService.extractModel(classifierConfig, modelInfo, spark, elasticSearchRDD);
        spark.stop();
    }

    private static void runClassifier(ModelInfo modelInfo) {
        SparkSession spark = setSparkEsConfigs();
//        JavaPairRDD<String, Map<String, Object>> elasticSearchRDD =
//                SparkUtility.getElasticSearchRDD(spark, classifierConfig.getEsIndex(), classifierConfig.getEsType());
        JavaSparkContext javaSparkContext = SparkUtility.getJavaSparkContext(spark);
        JavaPairRDD<String, Map<String, Object>> elasticSearchRDD = JavaEsSpark.esRDD(javaSparkContext, "keen/page", "?q=rotten tomatoes");
        JavaPairRDD<String, Map<String, Object>> parallelize = javaSparkContext.parallelizePairs(elasticSearchRDD.take(100));
        ClassifierService.classify(classifierConfig, spark, parallelize, modelInfo);
        spark.stop();
    }


    private static SparkSession setSparkEsConfigs() {
        SparkSession spark = SparkUtility.getSpark(classifierConfig.getAppName() + "-"
                + classifierConfig.getAppMode(), true);
        spark.sparkContext().conf().set("es.nodes", classifierConfig.getEsNodes());
        spark.sparkContext().conf().set("es.write.operation", classifierConfig.getEsWriteOperation());
        spark.sparkContext().conf().set("es.mapping.id", "id");
        spark.sparkContext().conf().set("es.index.auto.create", classifierConfig.getEsIndexAutoCreate());
        return spark;
    }

}
