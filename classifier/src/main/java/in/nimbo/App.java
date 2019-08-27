package in.nimbo;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.config.ProjectConfig;
import in.nimbo.dao.ElasticBulkListener;
import in.nimbo.common.entity.Page;
import in.nimbo.config.ClassifierConfig;
import in.nimbo.dao.ElasticDAO;
import in.nimbo.dao.ElasticDAOImpl;
import in.nimbo.entity.Category;
import in.nimbo.entity.Data;
import in.nimbo.entity.Link;
import in.nimbo.service.CrawlerService;
import in.nimbo.service.ParserService;
import in.nimbo.service.SampleExtractor;
import in.nimbo.service.ScheduleService;
import org.apache.http.HttpHost;
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
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) throws IOException, LangDetectException {
        ProjectConfig projectConfig = ProjectConfig.load();
        ClassifierConfig classifierConfig = ClassifierConfig.load();
        runCrawler(classifierConfig, projectConfig);
    }

    private static void runCrawler(ClassifierConfig classifierConfig, ProjectConfig projectConfig) throws IOException, LangDetectException {
        DetectorFactory.loadProfile("../conf/profiles");
        Cache<String, LocalDateTime> politenessCache = Caffeine.newBuilder().maximumSize(projectConfig.getCaffeineMaxSize())
                .expireAfterWrite(projectConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();
        Cache<String, LocalDateTime> crawlerCache = Caffeine.newBuilder().build();
        ElasticConfig elasticConfig = ElasticConfig.load();
        RestHighLevelClient client = initializeElasticSearchClient(elasticConfig);
        List<Page> backupPages = new ArrayList<>();
        ElasticBulkListener elasticBulkListener = new ElasticBulkListener();
        BulkProcessor bulkProcessor = initBulkElastic(elasticConfig, client, elasticBulkListener);
        ElasticDAO elasticDAO = new ElasticDAOImpl(elasticConfig, backupPages, bulkProcessor);
        ParserService parserService = new ParserService(projectConfig);
        ObjectMapper mapper = new ObjectMapper();
        List<Category> categories = loadFeed(mapper);
        Map<String, Integer> labelMap = loadLabels(categories);
        List<String> domains = loadDomains(categories);
        CrawlerService crawlerService = new CrawlerService(politenessCache, crawlerCache, parserService, elasticDAO, labelMap);
        BlockingQueue<Link> queue = new ArrayBlockingQueue<>(classifierConfig.getCrawlerQueueSize());
        fill(queue, categories);
        SampleExtractor sampleExtractor = new SampleExtractor(crawlerService, queue, domains, classifierConfig);
        ScheduleService scheduleService = new ScheduleService(sampleExtractor, classifierConfig);
        scheduleService.schedule();
    }

    private static RestHighLevelClient initializeElasticSearchClient(ElasticConfig elasticConfig) {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(elasticConfig.getHost(), elasticConfig.getPort()))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(elasticConfig.getConnectTimeout())
                        .setSocketTimeout(elasticConfig.getSocketTimeout()))
                .setMaxRetryTimeoutMillis(elasticConfig.getMaxRetryTimeoutMillis());
        return new RestHighLevelClient(restClientBuilder);
    }

    private static BulkProcessor initBulkElastic(ElasticConfig elasticConfig, RestHighLevelClient restHighLevelClient,
                                        ElasticBulkListener elasticBulkListener) {
        BulkProcessor.Builder builder = BulkProcessor.builder(
                (request, bulkListener) -> restHighLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                elasticBulkListener);
        builder.setBulkActions(elasticConfig.getBulkActions());
        builder.setBulkSize(new ByteSizeValue(elasticConfig.getBulkSize(), ByteSizeUnit.valueOf(elasticConfig.getBulkSizeUnit())));
        builder.setConcurrentRequests(elasticConfig.getConcurrentRequests());
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(elasticConfig.getBackoffDelaySeconds()),
                elasticConfig.getBackoffMaxRetry()));
        return builder.build();
    }

    private static void fill(BlockingQueue<Link> queue, List<Category> categories) {
        for (Category category : categories) {
            for (String site : category.getSites()) {
                try {
                    queue.put(new Link("https://" + site, category.getName(), 0));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static List<Category> loadFeed(ObjectMapper mapper) throws IOException {
        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("first-feed.json");
        Scanner scanner = new Scanner(resourceAsStream);
        StringBuilder stringBuilder = new StringBuilder("");
        while (scanner.hasNextLine()) {
            stringBuilder.append(scanner.nextLine());
        }
        String json = stringBuilder.toString();
        CollectionType collectionType = mapper.getTypeFactory().constructCollectionType(List.class, Category.class);
        return mapper.readValue(json, collectionType);
    }

    private static List<String> loadDomains(List<Category> categories) {
        List<String> domains = new ArrayList<>();
        for (Category category : categories) {
            domains.addAll(category.getSites());
        }
        return domains;
    }

    private static Map<String, Integer> loadLabels(List<Category> categories) {
        Map<String, Integer> map = new HashMap<>();
        for (Category category : categories) {
            map.put(category.getName(), map.size());
        }
        return map;
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

        Dataset<Row>[] tmp = features.randomSplit(new double[]{0.8, 0.2});
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
