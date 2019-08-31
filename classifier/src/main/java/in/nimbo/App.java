package in.nimbo;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.config.ProjectConfig;
import in.nimbo.common.dao.elastic.ElasticDAO;
import in.nimbo.common.dao.elastic.ElasticDAOImpl;
import in.nimbo.common.entity.Link;
import in.nimbo.common.service.ParserService;
import in.nimbo.config.ClassifierConfig;
import in.nimbo.entity.Category;
import in.nimbo.entity.Data;
import in.nimbo.service.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
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
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) throws IOException, LangDetectException {
        ProjectConfig projectConfig = ProjectConfig.load();
        ClassifierConfig classifierConfig = ClassifierConfig.load();
        if (classifierConfig.getAppMode() == ClassifierConfig.MODE.CRAWL) {
            runCrawler(classifierConfig, projectConfig);
        }
        else if (classifierConfig.getAppMode() == ClassifierConfig.MODE.CLASSIFY) {
            runClassifier(classifierConfig);
        }
    }

    private static void runCrawler(ClassifierConfig classifierConfig, ProjectConfig projectConfig) throws IOException, LangDetectException {
        DetectorFactory.loadProfile("../conf/profiles");
        Cache<String, LocalDateTime> politenessCache = Caffeine.newBuilder().maximumSize(projectConfig.getCaffeineMaxSize())
                .expireAfterWrite(projectConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();
        Cache<String, LocalDateTime> crawlerCache = Caffeine.newBuilder().build();

        ElasticConfig elasticConfig = ElasticConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();

        ElasticDAO elasticDAO = ElasticDAOImpl.createElasticDAO(elasticConfig, new CopyOnWriteArrayList<>());

        ObjectMapper mapper = new ObjectMapper();
        List<Category> categories = CrawlerService.loadFeed(mapper);
        Map<String, Double> labelMap = CrawlerService.loadLabels(categories);
        List<String> domains = CrawlerService.loadDomains(categories);
        BlockingQueue<Link> queue = new ArrayBlockingQueue<>(classifierConfig.getCrawlerQueueSize());
        CrawlerService.fillInitialCrawlQueue(queue, categories);

        Producer<String, Link> producer = new KafkaProducer<>(kafkaConfig.getTrainingProducerProperties());
        Consumer<String, Link> consumer = new KafkaConsumer<>(kafkaConfig.getTrainingConsumerProperties());
        consumer.subscribe(Collections.singleton(kafkaConfig.getTrainingTopic()));

        ParserService parserService = new ParserService(projectConfig);
        CrawlerService crawlerService = new CrawlerService(politenessCache, crawlerCache, parserService, elasticDAO, labelMap);
        KafkaConsumerService consumerService = new KafkaConsumerService(queue, kafkaConfig, consumer);
        KafkaProducerService producerService = new KafkaProducerService(kafkaConfig, producer);
        SampleExtractor sampleExtractor = new SampleExtractor(crawlerService, queue, domains, classifierConfig, producerService);
        ScheduleService scheduleService = new ScheduleService(sampleExtractor, consumerService, classifierConfig);
        scheduleService.schedule();

        Runtime.getRuntime().addShutdownHook(new Thread(scheduleService::stop));
    }

    private static void runClassifier(ClassifierConfig classifierConfig) {
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
                new Data((Long) tuple2._2.get("label"), (String) tuple2._2.get("content")));

        Dataset<Row> dataset = spark.createDataFrame(dataRDD, Data.class);

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

        Dataset<Row>[] tmp = features.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = tmp[0]; // training set
        Dataset<Row> test = tmp[1]; // test set

        NaiveBayesModel model = naiveBayes.train(training);
        model.set("modelType", classifierConfig.getNaiveBayesModelType());

        JavaPairRDD<Double, Double> predictionAndLabel =
                test.toJavaRDD().mapToPair((Row p) ->
                        new Tuple2<>(model.predict(p.getAs(1)), p.getDouble(0)));
        System.out.println(predictionAndLabel.collect());

        double accuracy =
                predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / (double) test.count();
        System.out.println(accuracy);

        // Save and load model
        try {
            model.save(classifierConfig.getNaiveBayesModelSaveLocation());
        } catch (IOException e) {
            e.printStackTrace();
        }
        NaiveBayesModel loadedModel = NaiveBayesModel.load(classifierConfig.getNaiveBayesModelSaveLocation());

        spark.stop();
    }
}
