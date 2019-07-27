package in.nimbo;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.config.*;
import in.nimbo.dao.elastic.ElasticBulkListener;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.elastic.ElasticDAOImpl;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.hbase.HBaseDAOImpl;
import in.nimbo.dao.redis.RedisDAO;
import in.nimbo.dao.redis.RedisDAOImpl;
import in.nimbo.entity.Page;
import in.nimbo.service.CrawlerService;
import in.nimbo.service.ParserService;
import in.nimbo.service.kafka.KafkaService;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class App {
    private static Logger logger = LoggerFactory.getLogger(App.class);
    private RestHighLevelClient restHighLevelClient;
    private KafkaService kafkaService;
    private ParserService parserService;
    private HBaseDAO hBaseDAO;
    private ElasticDAO elasticDAO;

    public App(RestHighLevelClient restHighLevelClient, KafkaService kafkaService, ParserService parserService, HBaseDAO hBaseDAO, ElasticDAO elasticDAO) {
        this.restHighLevelClient = restHighLevelClient;
        this.kafkaService = kafkaService;
        this.parserService = parserService;
        this.hBaseDAO = hBaseDAO;
        this.elasticDAO = elasticDAO;
    }

    public static void main(String[] args) {
        loadLanguageDetector();

        HBaseConfig hBaseConfig = HBaseConfig.load();
        AppConfig appConfig = AppConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();
        ElasticConfig elasticConfig = ElasticConfig.load();
        RedisConfig redisConfig = RedisConfig.load();
        logger.info("Configuration loaded");

        JedisCluster cluster = new JedisCluster(redisConfig.getHostAndPorts());
        logger.info("Redis started");

        List<Page> backupPages = new ArrayList<>();
        RestHighLevelClient restHighLevelClient = initializeElasticSearchClient(elasticConfig);
        ElasticBulkListener elasticBulkListener = new ElasticBulkListener(backupPages);
        BulkProcessor bulkProcessor = initializeElasticSearchBulk(elasticConfig, restHighLevelClient, elasticBulkListener);
        ElasticDAO elasticDAO = new ElasticDAOImpl(elasticConfig, bulkProcessor, backupPages);
        elasticBulkListener.setElasticDAO(elasticDAO);

        Connection hBaseConnection = null;
        try {
            hBaseConnection = ConnectionFactory.createConnection();
            logger.info("HBase started");
        } catch (IOException e) {
            logger.error("Unable to establish HBase connection", e);
            System.exit(1);
        }

        HBaseDAO hBaseDAO = new HBaseDAOImpl(hBaseConnection, hBaseConfig);
        RedisDAO redisDAO = new RedisDAOImpl(cluster, redisConfig);
        logger.info("DAO interface created");

        ParserService parserService = new ParserService(appConfig);
        Cache<String, LocalDateTime> cache = Caffeine.newBuilder().maximumSize(appConfig.getCaffeineMaxSize())
                .expireAfterWrite(appConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();
        CrawlerService crawlerService = new CrawlerService(cache, hBaseDAO, elasticDAO, parserService, redisDAO);
        KafkaService kafkaService = new KafkaService(crawlerService, kafkaConfig);


        logger.info("Application started");
        App app = new App(restHighLevelClient, kafkaService, parserService, hBaseDAO, elasticDAO);
        Runtime.getRuntime().addShutdownHook(new Thread(app::stopApp));
        app.startApp();
    }

    private static void loadLanguageDetector() {
        try {
            logger.info("Load application profiles for language detector");
            DetectorFactory.loadProfile("profiles");
        } catch (LangDetectException e) {
            System.out.println("Unable to load profiles of language detector. Provide \"profile\" folder for language detector.");
            System.exit(1);
        }
    }

    private static RestHighLevelClient initializeElasticSearchClient(ElasticConfig elasticConfig) {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(elasticConfig.getHost(), elasticConfig.getPort()))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(elasticConfig.getConnectTimeout())
                        .setSocketTimeout(elasticConfig.getSocketTimeout()))
                .setMaxRetryTimeoutMillis(elasticConfig.getMaxRetryTimeoutMillis());
        return new RestHighLevelClient(restClientBuilder);
    }

    private static BulkProcessor initializeElasticSearchBulk(ElasticConfig elasticConfig, RestHighLevelClient restHighLevelClient,
                                                             ElasticBulkListener elasticBulkListener) {
        BulkProcessor.Builder builder = BulkProcessor.builder(
                (request, bulkListener) -> restHighLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                elasticBulkListener);
        builder.setBulkActions(elasticConfig.getBulkActions());
        builder.setBulkSize(new ByteSizeValue(elasticConfig.getBulkSize(), ByteSizeUnit.valueOf(elasticConfig.getBulkSizeUnit())));
        builder.setConcurrentRequests(elasticConfig.getConcurrentRequests());
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(elasticConfig.getBackoffDelaySeconds()),
                elasticConfig.getBackoffMaxRetry()));
        BulkProcessor bulkProcessor = builder.build();
        logger.info("ElasticSearch started");
        return bulkProcessor;
    }

    private void runSyncWithElastic() {
        hBaseDAO.syncWithElastic(elasticDAO, parserService);
    }

    private void startApp() {
        kafkaService.schedule();
        logger.info("Schedule service started");
        System.out.println("Welcome to Search Engine");
        System.out.print("engine> ");
        Scanner in = new Scanner(System.in);
        while (in.hasNext()) {
            String cmd = in.next();
            if (cmd.equals("add")) {
                String link = in.next();
                kafkaService.sendMessage(link);
            } else if (cmd.equals("exit")) {
                stopApp();
                break;
            }
            System.out.print("engine> ");
        }
    }

    private void stopApp() {
        kafkaService.stopSchedule();
        try {
            restHighLevelClient.close();
            hBaseDAO.close();
        } catch (IOException e) {
            logger.warn("Unable to close resources", e);
        }
    }
}
