package in.nimbo.service;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.ElasticException;
import in.nimbo.common.exception.HBaseException;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class CollectorService {
    private Logger logger = LoggerFactory.getLogger("collector");
    private HBaseDAO hBaseDAO;
    private ElasticDAO elasticDAO;

    private Timer hBaseAddTimer;
    private Timer ElasticsearchAdd;

    public CollectorService(HBaseDAO hBaseDAO, ElasticDAO elasticDAO) {
        this.hBaseDAO = hBaseDAO;
        this.elasticDAO = elasticDAO;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        hBaseAddTimer = metricRegistry.timer(MetricRegistry.name(CollectorService.class, "HBaseAdd"));
        ElasticsearchAdd = metricRegistry.timer(MetricRegistry.name(CollectorService.class, "ElasticsearchAdd"));
    }

    public boolean processList(List<Page> bufferList) {
        List<Page> filtered = bufferList.stream().filter(page -> !page.getAnchors().isEmpty()).collect(Collectors.toList());
        try {
            Timer.Context hBaseAddTimerContext = hBaseAddTimer.time();
            hBaseDAO.add(filtered);
            hBaseAddTimerContext.stop();
            Timer.Context ElasticsearchAddContext = ElasticsearchAdd.time();
            for (Page page : bufferList) {
                elasticDAO.save(page);
            }
            ElasticsearchAddContext.stop();
        } catch (HBaseException | ElasticException e) {
            logger.error("Unable to establish connection", e);
            logger.info("Retry link again because of exception");
        }
        return false;
    }
}
