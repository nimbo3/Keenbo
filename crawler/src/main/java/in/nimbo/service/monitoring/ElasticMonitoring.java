package in.nimbo.service.monitoring;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.common.config.ProjectConfig;
import in.nimbo.dao.elastic.ElasticDAO;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ElasticMonitoring {
    private ScheduledExecutorService executorService;
    private ElasticDAO elasticDAO;
    private ProjectConfig projectConfig;
    private Histogram histogram;

    public ElasticMonitoring(ElasticDAO elasticDAO, ProjectConfig projectConfig) {
        this.elasticDAO = elasticDAO;
        this.projectConfig = projectConfig;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        histogram = metricRegistry.histogram(MetricRegistry.name(ElasticMonitoring.class, "PageCount"));
    }

    public void monitor() {
        executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(() -> {
            long count = elasticDAO.count();
            histogram.update(count);
        }, 0, projectConfig.getMonitoringPeriod(), TimeUnit.SECONDS);
    }

    public void stopMonitor() {
        executorService.shutdown();
    }
}
