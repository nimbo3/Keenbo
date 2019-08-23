package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.config.ProjectConfig;
import in.nimbo.common.config.RedisConfig;
import in.nimbo.config.ShufflerConfig;
import in.nimbo.redis.RedisDAOImpl;
import in.nimbo.service.kafka.KafkaService;
import in.nimbo.service.kafka.KafkaServiceImpl;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class App {
    private static Logger cliLogger = LoggerFactory.getLogger("cli");
    private static Logger appLogger = LoggerFactory.getLogger("shuffler");
    private KafkaService kafkaService;
    private RedissonClient redissonClient;


    public App(KafkaService kafkaService, RedissonClient redis) {
        this.kafkaService = kafkaService;
        this.redissonClient = redis;
    }

    public static void main(String[] args) {
        ProjectConfig projectConfig = ProjectConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();
        RedisConfig redisConfig = RedisConfig.load();
        ShufflerConfig shufflerConfig = ShufflerConfig.load();
        appLogger.info("Configuration loaded");

        initReporter(projectConfig);
        appLogger.info("Reporter started");

        String[] addresses = redisConfig.getHostAndPorts().stream()
                .map(hostAndPort -> "redis://" + hostAndPort.getHost() + ":" + hostAndPort.getPort())
                .toArray(String[]::new);
        Config config = new Config();
        config.useClusterServers().addNodeAddress(addresses);
        RedissonClient redis = Redisson.create(config);
        RedisDAOImpl redisDAO = new RedisDAOImpl(redis);
        appLogger.info("Redis started");

        KafkaService kafkaService = new KafkaServiceImpl(kafkaConfig, shufflerConfig, redisDAO);
        appLogger.info("Services started");

        appLogger.info("Application started");
        App app = new App(kafkaService,  redis);
        Runtime.getRuntime().addShutdownHook(new Thread(app::stopApp));

        app.startApp();
    }

    private void startApp() {
        kafkaService.schedule();
        appLogger.info("Schedule service started");
        cliLogger.info("Welcome to Shuffler\n");
    }

    private void stopApp() {
        kafkaService.stopSchedule();
        redissonClient.shutdown();
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
