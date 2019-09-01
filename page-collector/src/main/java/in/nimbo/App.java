package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.config.HBaseConfig;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.config.ProjectConfig;
import in.nimbo.common.dao.elastic.ElasticDAO;
import in.nimbo.common.dao.elastic.ElasticDAOImpl;
import in.nimbo.common.dao.hbase.HBaseDAO;
import in.nimbo.common.dao.hbase.HBaseDAOImpl;
import in.nimbo.common.entity.Page;
import in.nimbo.config.CollectorConfig;
import in.nimbo.service.CollectorService;
import in.nimbo.service.kafka.KafkaService;
import in.nimbo.service.kafka.KafkaServiceImpl;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class App {
    private static Logger cliLogger = LoggerFactory.getLogger("cli");
    private static Logger appLogger = LoggerFactory.getLogger("collector");
    private ElasticDAO elasticDAO;
    private KafkaService kafkaService;
    private HBaseDAO hBaseDAO;

    public App(KafkaService kafkaService, ElasticDAO elasticDAO, HBaseDAO hBaseDAO) {
        this.kafkaService = kafkaService;
        this.elasticDAO = elasticDAO;
        this.hBaseDAO = hBaseDAO;
    }

    public static void main(String[] args) {
        HBaseConfig hBasePageConfig = HBaseConfig.load();
        ProjectConfig projectConfig = ProjectConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();
        ElasticConfig elasticConfig = ElasticConfig.load();
        CollectorConfig collectorConfig = CollectorConfig.load();
        appLogger.info("Configuration loaded");

        initReporter(projectConfig);
        appLogger.info("Reporter started");

        CopyOnWriteArrayList<Page> backupPages = new CopyOnWriteArrayList<>();
        ElasticDAO elasticDAO = ElasticDAOImpl.createElasticDAO(elasticConfig, backupPages);
        appLogger.info("ElasticSearch started");

        Connection hBaseConnection = null;
        try {
            hBaseConnection = ConnectionFactory.createConnection();
            appLogger.info("HBase started");
        } catch (IOException e) {
            appLogger.error("Unable to establish HBase connection", e);
            System.exit(1);
        }

        HBaseDAO hBaseDAO = new HBaseDAOImpl(hBaseConnection, hBasePageConfig);
        appLogger.info("DAO interface created");
        CollectorService collectorService = new CollectorService(hBaseDAO, elasticDAO, collectorConfig.isExtractKeywordEnabled());
        KafkaService kafkaService = new KafkaServiceImpl(kafkaConfig, collectorService);
        appLogger.info("Services started");

        appLogger.info("Application started");
        App app = new App(kafkaService, elasticDAO, hBaseDAO);
        Runtime.getRuntime().addShutdownHook(new Thread(app::stopApp));

        app.startApp();
    }

    private void startApp() {
        kafkaService.schedule();
        appLogger.info("Schedule service started");
        cliLogger.info("Welcome to Page Collector\n");
    }

    private void stopApp() {
        kafkaService.stopSchedule();
        try {
            elasticDAO.close();
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
