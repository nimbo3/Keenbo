package in.nimbo.service.monitoring;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.dao.elastic.ElasticDAO;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ElasticMonitoring {
    private ElasticDAO elasticDAO;
    private Histogram histogram;

    public ElasticMonitoring(ElasticDAO elasticDAO) {
        this.elasticDAO = elasticDAO;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        histogram = metricRegistry.histogram(MetricRegistry.name(ElasticMonitoring.class,"Page count"));
    }

    public void schedule() {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(() -> {
            long count = elasticDAO.count();
            histogram.update(count);
        }, 0, 10, TimeUnit.SECONDS);
        executorService.shutdown();
    }
}
