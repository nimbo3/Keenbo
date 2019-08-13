package in.nimbo.service;

import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.HBaseException;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import org.elasticsearch.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectorService {
    private Logger logger = LoggerFactory.getLogger("app");
    private HBaseDAO hBaseDAO;
    private ElasticDAO elasticDAO;

    public CollectorService(HBaseDAO hBaseDAO, ElasticDAO elasticDAO) {
        this.hBaseDAO = hBaseDAO;
        this.elasticDAO = elasticDAO;
    }

    public boolean handle(Page page) {
        try {
            boolean isAddedToHBase;
            if (page.getAnchors().isEmpty()) {
                isAddedToHBase = true;
            } else {
                isAddedToHBase = hBaseDAO.add(page);
            }
            if (isAddedToHBase) {
                elasticDAO.save(page);
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
