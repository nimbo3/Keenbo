package in.nimbo.service;

import in.nimbo.config.ClassifierConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.util.Map;

public class ClassifierService {
    private ClassifierService() {
    }

    public static void classify(ClassifierConfig classifierConfig, SparkSession spark,
                                JavaPairRDD<String, Map<String, Object>> elasticSearchRDD, ModelInfo modelInfo) {
        NaiveBayesModel naiveBayesModel = NaiveBayesModel.load(classifierConfig.getNaiveBayesModelSaveLocation());
        naiveBayesModel.setFeaturesCol("feature");
        naiveBayesModel.setPredictionCol("label");
        IDFModel idfModel = IDFModel.load(classifierConfig.getNaiveBayesIDFSaveLocation());

        JavaRDD<Row> dataRDD = elasticSearchRDD.map(tuple2 -> RowFactory.create(tuple2._1, tuple2._2.get("content")));
        StructType esStruct = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("content", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> dataset = spark.createDataFrame(dataRDD, esStruct);

        StringIndexer stringIndexer = new StringIndexer().setInputCol("content").setOutputCol("what");

        Tokenizer tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words");
        StopWordsRemover stopWordsRemover = new StopWordsRemover().setInputCol("words")
                .setOutputCol("words-filtered")
                .setStopWords(modelInfo.getStopWords());
        HashingTF hashingTF = new HashingTF()
                .setInputCol("words-filtered")
                .setOutputCol("rawFeatures")
                .setNumFeatures(classifierConfig.getHashingNumFeatures());

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]
                {tokenizer, stopWordsRemover, stringIndexer, hashingTF, idfModel, naiveBayesModel});

        PipelineModel pipelineModel = pipeline.fit(dataset);
        Dataset<Row> rescaledData = pipelineModel.transform(dataset);
        rescaledData.select("what").show(false);

        JavaPairRDD<String, Double> predictionAndLabel = rescaledData.select("id", "label")
                .toJavaRDD()
                .mapToPair(row -> new Tuple2<>(row.getString(0), row.getDouble(1)));

        JavaRDD<Map<String, Object>> join = elasticSearchRDD.join(predictionAndLabel)
                .map(tuple2 -> {
                    Map<String, Object> map = tuple2._2._1;
                    map.put("id", tuple2._1);
                    map.put("label", modelInfo.getLabelString(tuple2._2._2));
                    return map;
                });

        JavaEsSpark.saveToEs(join, classifierConfig.getEsOutputIndex() + "/" + classifierConfig.getEsType());
    }
}
