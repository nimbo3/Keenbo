package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.config.*;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.elastic.ElasticDAOImpl;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.hbase.HBaseDAOImpl;
import in.nimbo.dao.redis.RedisDAO;
import in.nimbo.dao.redis.RedisDAOImpl;
import in.nimbo.service.CrawlerService;
import in.nimbo.service.ParserService;
import in.nimbo.service.kafka.KafkaService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class App {
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        try {
            logger.info("Load application profiles for language detector");
            DetectorFactory.loadProfile("crawler/src/main/resources/profiles");
        } catch (LangDetectException e) {
            System.out.println("Unable to load profiles of language detector. Provide \"profile\" folder for language detector.");
            logger.info("Unable to load profiles of language detector.");
            System.exit(1);
        }

        Configuration configuration = HBaseConfiguration.create();
        HBaseConfig hBaseConfig = HBaseConfig.load();
        AppConfig appConfig = AppConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();
        ElasticConfig elasticConfig = ElasticConfig.load();
        RedisConfig redisConfig = RedisConfig.load();
        JedisCluster cluster = new JedisCluster(redisConfig.getHostAndPorts());
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticConfig.getHost(), elasticConfig.getPort())));
        logger.info("Configuration loaded");

        ElasticDAO elasticDAO = new ElasticDAOImpl(restHighLevelClient, elasticConfig);
        HBaseDAO hBaseDAO = new HBaseDAOImpl(configuration, hBaseConfig);
        RedisDAO redisDAO = new RedisDAOImpl(cluster, redisConfig);

        Cache<String, LocalDateTime> cache = Caffeine.newBuilder().maximumSize(appConfig.getCaffeineMaxSize())
                .expireAfterWrite(appConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();
        startReporter();

        ParserService parserService = new ParserService(appConfig);
        CrawlerService crawlerService = new CrawlerService(cache, hBaseDAO, elasticDAO, parserService, redisDAO);
        KafkaService kafkaService = new KafkaService(crawlerService, kafkaConfig);
        logger.info("Start schedule service");

        kafkaService.schedule();

        logger.info("Application started");
        System.out.println("Welcome to Search Engine");
        System.out.print("engine> ");
        Scanner in = new Scanner(System.in);
        while (in.hasNext()) {
            String cmd = in.next();
            if (cmd.equals("add")) {
                String link = in.next();
                kafkaService.sendMessage(link);
            } else if (cmd.equals("exit")) {
                kafkaService.stopSchedule();
                restHighLevelClient.close();
                break;
            }
            System.out.print("engine> ");
        }
    }

    private static void startReporter() {
        MetricRegistry metricRegistry = SharedMetricRegistries.setDefault("Keenbo");
        JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).build();
        reporter.start();
    }
}
