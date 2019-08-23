package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.config.ProjectConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.dao.elastic.ElasticBulkListener;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.elastic.ElasticDAOImpl;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.hbase.HBaseDAOImpl;
import in.nimbo.service.CollectorService;
import in.nimbo.service.kafka.KafkaService;
import in.nimbo.service.kafka.KafkaServiceImpl;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class App {
    private static Logger cliLogger = LoggerFactory.getLogger("cli");
    private static Logger appLogger = LoggerFactory.getLogger("collector");
    private RestHighLevelClient restHighLevelClient;
    private KafkaService kafkaService;
    private HBaseDAO hBaseDAO;


    public App(RestHighLevelClient restHighLevelClient, KafkaService kafkaService, HBaseDAO hBaseDAO) {
        this.restHighLevelClient = restHighLevelClient;
        this.kafkaService = kafkaService;
        this.hBaseDAO = hBaseDAO;
    }

    public static void main(String[] args) {
        HBaseConfig hBaseConfig = HBaseConfig.load();
        ProjectConfig projectConfig = ProjectConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();
        ElasticConfig elasticConfig = ElasticConfig.load();
        appLogger.info("Configuration loaded");

        initReporter(projectConfig);
        appLogger.info("Reporter started");

        List<Page> backupPages = new ArrayList<>();
        RestHighLevelClient restHighLevelClient = initializeElasticSearchClient(elasticConfig);
        ElasticBulkListener elasticBulkListener = new ElasticBulkListener(backupPages);
        BulkProcessor bulkProcessor = initializeElasticSearchBulk(elasticConfig, restHighLevelClient, elasticBulkListener);
        ElasticDAO elasticDAO = new ElasticDAOImpl(elasticConfig, bulkProcessor, backupPages, restHighLevelClient);
        elasticBulkListener.setElasticDAO(elasticDAO);
        appLogger.info("ElasticSearch started");

        Connection hBaseConnection = null;
        try {
            hBaseConnection = ConnectionFactory.createConnection();
            appLogger.info("HBase started");
        } catch (IOException e) {
            appLogger.error("Unable to establish HBase connection", e);
            System.exit(1);
        }

        HBaseDAO hBaseDAO = new HBaseDAOImpl(hBaseConnection, hBaseConfig);
        appLogger.info("DAO interface created");

        CollectorService collectorService = new CollectorService(hBaseDAO, elasticDAO);
        KafkaService kafkaService = new KafkaServiceImpl(kafkaConfig, collectorService);
        appLogger.info("Services started");

        appLogger.info("Application started");
        App app = new App(restHighLevelClient, kafkaService, hBaseDAO);
        Runtime.getRuntime().addShutdownHook(new Thread(app::stopApp));

        app.startApp();
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
        return builder.build();
    }

    private void startApp() {
        kafkaService.schedule();
        appLogger.info("Schedule service started");
        cliLogger.info("Welcome to Page Collector\n");
    }

    private void stopApp() {
        kafkaService.stopSchedule();
        try {
            restHighLevelClient.close();
            hBaseDAO.close();
        } catch (IOException e) {
            appLogger.warn("Unable to close resources", e);
        }
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
