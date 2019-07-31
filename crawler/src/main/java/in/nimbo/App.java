package in.nimbo;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
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
import in.nimbo.service.monitoring.ElasticMonitoring;
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class App {
    private static Logger logger = LoggerFactory.getLogger("app");
    private RestHighLevelClient restHighLevelClient;
    private KafkaService kafkaService;
    private HBaseDAO hBaseDAO;
    private JedisCluster cluster;

    public App(RestHighLevelClient restHighLevelClient, KafkaService kafkaService, HBaseDAO hBaseDAO, JedisCluster cluster) {
        this.restHighLevelClient = restHighLevelClient;
        this.kafkaService = kafkaService;
        this.hBaseDAO = hBaseDAO;
        this.cluster = cluster;
    }

    public static void main(String[] args) {
        loadLanguageDetector();

        HBaseConfig hBaseConfig = HBaseConfig.load();
        AppConfig appConfig = AppConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();
        ElasticConfig elasticConfig = ElasticConfig.load();
        RedisConfig redisConfig = RedisConfig.load();
        logger.info("Configuration loaded");

        initReporter(appConfig);
        logger.info("Reporter started");

        JedisCluster cluster = new JedisCluster(redisConfig.getHostAndPorts());
        logger.info("Redis started");

        List<Page> backupPages = new ArrayList<>();
        RestHighLevelClient restHighLevelClient = initializeElasticSearchClient(elasticConfig);
        ElasticBulkListener elasticBulkListener = new ElasticBulkListener(backupPages);
        BulkProcessor bulkProcessor = initializeElasticSearchBulk(elasticConfig, restHighLevelClient, elasticBulkListener);
        ElasticDAO elasticDAO = new ElasticDAOImpl(elasticConfig, bulkProcessor, backupPages, restHighLevelClient);
        elasticBulkListener.setElasticDAO(elasticDAO);

        ElasticMonitoring elasticMonitoring = new ElasticMonitoring(elasticDAO);
        elasticMonitoring.schedule();

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

        Cache<String, LocalDateTime> cache = Caffeine.newBuilder().maximumSize(appConfig.getCaffeineMaxSize())
                .expireAfterWrite(appConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();

        ParserService parserService = new ParserService(appConfig);
        CrawlerService crawlerService = new CrawlerService(cache, hBaseDAO, elasticDAO, parserService, redisDAO);
        KafkaService kafkaService = new KafkaService(crawlerService, kafkaConfig);

        logger.info("Application started");
        App app = new App(restHighLevelClient, kafkaService, hBaseDAO, cluster);
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

    public static RestHighLevelClient initializeElasticSearchClient(ElasticConfig elasticConfig) {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(elasticConfig.getHost(), elasticConfig.getPort()))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(elasticConfig.getConnectTimeout())
                        .setSocketTimeout(elasticConfig.getSocketTimeout()))
                .setMaxRetryTimeoutMillis(elasticConfig.getMaxRetryTimeoutMillis());
        return new RestHighLevelClient(restClientBuilder);
    }

    public static BulkProcessor initializeElasticSearchBulk(ElasticConfig elasticConfig, RestHighLevelClient restHighLevelClient,
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
            cluster.close();
        } catch (IOException e) {
            logger.warn("Unable to close resources", e);
        }
    }

    private static void initReporter(AppConfig appConfig) {
        String hostName = appConfig.getReportName();
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.info("Unable to detect host name. Use default value");
        }
        MetricRegistry metricRegistry = SharedMetricRegistries.setDefault(appConfig.getReportName());
        Graphite graphite = new Graphite(new InetSocketAddress(appConfig.getReportHost(), appConfig.getReportPort()));
        GraphiteReporter reporter = GraphiteReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.MILLISECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .prefixedWith(hostName)
                .filter(MetricFilter.ALL)
                .build(graphite);
        reporter.start(appConfig.getReportPeriod(), TimeUnit.SECONDS);
    }
}
