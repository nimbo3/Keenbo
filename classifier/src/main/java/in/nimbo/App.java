package in.nimbo;

import in.nimbo.config.ClassifierConfig;
import in.nimbo.entity.Data;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.IOException;
import java.util.Map;

public class App {
    public static void main(String[] args) {
        ClassifierConfig classifierConfig = ClassifierConfig.load();

        SparkSession spark = SparkSession.builder()
                .appName(classifierConfig.getAppName())
                .master("local")
                .getOrCreate();
        spark.sparkContext().conf().set("es.nodes", classifierConfig.getEsNodes());
        spark.sparkContext().conf().set("es.write.operation", classifierConfig.getEsWriteOperation());
        spark.sparkContext().conf().set("es.mapping.id", "id");
        spark.sparkContext().conf().set("es.index.auto.create", classifierConfig.getEsIndexAutoCreate());

        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(javaSparkContext,
                classifierConfig.getEsIndex() + "/" + classifierConfig.getEsType());

        JavaRDD<Data> dataRDD = esRDD.map(tuple2 ->
                new Data((String) tuple2._2.get("label"), (String) tuple2._2.get("content")));

        Dataset<Row> dataset = spark.createDataFrame(dataRDD, Data.class);
        dataset.show(false);

        Tokenizer tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(dataset);

        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures")
                .setNumFeatures(classifierConfig.getHashingNumFeatures());

        Dataset<Row> featurizedData = hashingTF.transform(wordsData);

        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("feature");
        IDFModel idfModel = idf.fit(featurizedData);

        Dataset<Row> rescaledData = idfModel.transform(featurizedData);
        Dataset<Row> features = rescaledData.select("label", "feature");

        NaiveBayes naiveBayes = new NaiveBayes()
                .setModelType(classifierConfig.getNaiveBayesModelType())
                .setLabelCol("label")
                .setFeaturesCol("feature");

        Dataset<Row>[] tmp = features.randomSplit(new double[]{0.5, 0.5});
        Dataset<Row> training = tmp[0]; // training set
        Dataset<Row> test = tmp[1]; // test set

        NaiveBayesModel model = naiveBayes.train(training);
        model.set("modelType", classifierConfig.getNaiveBayesModelType());

        JavaPairRDD<Double, Double> predictionAndLabel =
                test.toJavaRDD().mapToPair((Row p) ->
                        new Tuple2<>(model.predict(p.getAs(1)), p.getDouble(0)));
        test.show(false);
        System.out.println(predictionAndLabel.collect());

        double accuracy =
                predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / (double) test.count();
        System.out.println(accuracy);

        // Save and load model
//        try {
//            model.save(classifierConfig.getNaiveBayesModelSaveLocation());
//        } catch (IOException e) {}
//        NaiveBayesModel loadedModel = NaiveBayesModel.load(classifierConfig.getNaiveBayesModelSaveLocation());

        spark.stop();
    }
}
