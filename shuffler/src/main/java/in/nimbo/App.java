package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.config.ProjectConfig;
import in.nimbo.config.ShufflerConfig;
import in.nimbo.service.kafka.KafkaService;
import in.nimbo.service.kafka.KafkaServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class App {
    private static Logger cliLogger = LoggerFactory.getLogger("cli");
    private static Logger appLogger = LoggerFactory.getLogger("shuffler");
    private KafkaService kafkaService;

    public App(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    public static void main(String[] args) {
        ProjectConfig projectConfig = ProjectConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();
        ShufflerConfig shufflerConfig = ShufflerConfig.load();
        appLogger.info("Configuration loaded");

        initReporter(projectConfig);
        appLogger.info("Reporter started");

        KafkaService kafkaService = new KafkaServiceImpl(kafkaConfig, shufflerConfig);
        appLogger.info("Services started");

        appLogger.info("Application started");
        App app = new App(kafkaService);
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
