package in.nimbo.service;

import in.nimbo.config.ClassifierConfig;
import in.nimbo.entity.Data;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.Map;

public class ClassifierService {
    private ClassifierService() {
    }

    public static void extractModel(ClassifierConfig classifierConfig, SparkSession spark,
                             JavaPairRDD<String, Map<String, Object>> elasticSearchRDD) {
        JavaRDD<Data> dataRDD = elasticSearchRDD.map(tuple2 ->
                new Data((Long) tuple2._2.get("label"), (String) tuple2._2.get("content")));
        Dataset<Row> dataset = spark.createDataFrame(dataRDD, Data.class);

        Tokenizer tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(dataset);

        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures")
                .setNumFeatures(classifierConfig.getHashingNumFeatures());
        Dataset<Row> featuredData = hashingTF.transform(wordsData);

        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("feature");
        IDFModel idfModel = idf.fit(featuredData);
        try {
            idf.save(classifierConfig.getNaiveBayesIDFSaveLocation());
        } catch (IOException e) {
            System.out.println("Unable to save idf model: " + e.getMessage());
        }

        Dataset<Row> rescaledData = idfModel.transform(featuredData);
        Dataset<Row> features = rescaledData.select("label", "feature");

        NaiveBayes naiveBayes = new NaiveBayes()
                .setModelType(classifierConfig.getNaiveBayesModelType())
                .setLabelCol("label")
                .setFeaturesCol("feature");

        Dataset<Row>[] tmp = features.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = tmp[0];
        Dataset<Row> test = tmp[1];

        NaiveBayesModel model = naiveBayes.train(training);
        model.set("modelType", classifierConfig.getNaiveBayesModelType());

        JavaPairRDD<Double, Double> predictionAndLabel =
                test.toJavaRDD().mapToPair((Row p) -> new Tuple2<>(model.predict(p.getAs(1)), p.getDouble(0)));
        System.out.println(predictionAndLabel.collect());

        double accuracy = predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / (double) test.count();
        System.out.println(accuracy);

        try {
            model.save(classifierConfig.getNaiveBayesModelSaveLocation());
        } catch (IOException e) {
            System.out.println("Unable to save naive bayes model: " + e.getMessage());
        }
        NaiveBayesModel loadedModel = NaiveBayesModel.load(classifierConfig.getNaiveBayesModelSaveLocation());
    }
}
