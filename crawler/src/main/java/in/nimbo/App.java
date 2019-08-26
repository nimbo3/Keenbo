package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.config.ProjectConfig;
import in.nimbo.common.config.RedisConfig;
import in.nimbo.dao.redis.RedisDAOImpl;
import in.nimbo.service.CrawlerService;
import in.nimbo.service.ParserService;
import in.nimbo.service.kafka.KafkaService;
import in.nimbo.service.kafka.KafkaServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.time.LocalDateTime;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class App {
    private static Logger cliLogger = LoggerFactory.getLogger("cli");
    private static Logger appLogger = LoggerFactory.getLogger("crawler");
    private KafkaService kafkaService;

    public App(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    public static void main(String[] args){
        loadLanguageDetector();

        ProjectConfig projectConfig = ProjectConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();
        appLogger.info("Configuration loaded");

        initReporter(projectConfig);
        appLogger.info("Reporter started");

        RedisConfig redisConfig = RedisConfig.load();
        JedisCluster redisCluster = new JedisCluster(redisConfig.getHostAndPorts());
        RedisDAOImpl redisDAO = new RedisDAOImpl(redisCluster, redisConfig);
        appLogger.info("Redis started");

        Cache<String, LocalDateTime> cache = Caffeine.newBuilder().maximumSize(projectConfig.getCaffeineMaxSize())
                .expireAfterWrite(projectConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();

        ParserService parserService = new ParserService(projectConfig);
        CrawlerService crawlerService = new CrawlerService(cache, redisDAO, parserService);
        KafkaService kafkaService = new KafkaServiceImpl(crawlerService, kafkaConfig);
        appLogger.info("Services started");

        appLogger.info("Application started");
        App app = new App(kafkaService);
        Runtime.getRuntime().addShutdownHook(new Thread(app::stopApp));

        app.startApp();
    }

    private static void loadLanguageDetector() {
        try {
            appLogger.info("Load application profiles for language detector");
            DetectorFactory.loadProfile("../conf/profiles");
        } catch (LangDetectException e) {
            cliLogger.info("Unable to load profiles of language detector. Provide \"profile\" folder for language detector.\n");
            System.exit(1);
        }
    }

    private void startApp() {
        kafkaService.schedule();
        appLogger.info("Schedule service started");
        cliLogger.info("Welcome to Search Engine\n");
        cliLogger.info("engine> ");
        Scanner in = new Scanner(System.in);
        while (in.hasNext()) {
            String cmd = in.next();
            if (cmd.equals("add")) {
                String link = in.next();
                kafkaService.sendMessage(link);
                cliLogger.info("Site {} added\n", link);
            } else if (cmd.equals("exit")) {
                stopApp();
                break;
            }
            cliLogger.info("engine> ");
        }
    }

    private void stopApp() {
        kafkaService.stopSchedule();
        appLogger.info("Application stopped");
    }

    private static void initReporter(ProjectConfig projectConfig) {
        MetricRegistry metricRegistry = SharedMetricRegistries.setDefault(projectConfig.getReportName());
        JmxReporter reporter = JmxReporter.forRegistry(metricRegistry)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .convertRatesTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start();
    }
}
