package in.nimbo.service;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.HBaseException;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import org.elasticsearch.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public boolean handle(Page page) {
        try {
            boolean isAddedToHBase;
            if (page.getAnchors().isEmpty()) {
                isAddedToHBase = true;
            } else {
                Timer.Context hBaseAddTimerContext = hBaseAddTimer.time();
                isAddedToHBase = hBaseDAO.add(page);
                hBaseAddTimerContext.stop();
            }
            if (isAddedToHBase) {
                Timer.Context ElasticsearchAddContext = ElasticsearchAdd.time();
                elasticDAO.save(page);
                ElasticsearchAddContext.stop();
                return true;
            } else {
                logger.warn("Unable to add page with link {} to HBase", page.getLink());
            }
        } catch (HBaseException | ElasticsearchException e) {
            logger.error("Unable to establish connection", e);
            logger.info("Retry link {} again because of exception", page.getLink());
        }
        return false;
    }
}
