package in.nimbo.service.monitoring;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.config.AppConfig;
import in.nimbo.dao.redis.RedisDAO;
import in.nimbo.entity.RedisNodeStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RedisMonitoring {
    private RedisDAO redisDAO;
    private AppConfig appConfig;
    private Map<String, Histogram> histograms;

    public RedisMonitoring(RedisDAO redisDAO, AppConfig appConfig) {
        this.redisDAO = redisDAO;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        List<RedisNodeStatus> statuses = redisDAO.memoryUsage();
        histograms = new HashMap<>();
        for (RedisNodeStatus status : statuses) {
            Histogram histogram = metricRegistry.histogram(MetricRegistry.name(ElasticMonitoring.class,"Redis memory usage " + status.getNode()));
            histograms.put(status.getNode(), histogram);
        }
        this.appConfig = appConfig;
    }

    public void monitor() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(() -> {
            List<RedisNodeStatus> statuses = redisDAO.memoryUsage();
            for (RedisNodeStatus status : statuses) {
                histograms.get(status.getNode()).update(status.getValue());
            }
        }, 0, appConfig.getMonitoringPeriod(), TimeUnit.SECONDS);
        executorService.shutdown();
    }
}
