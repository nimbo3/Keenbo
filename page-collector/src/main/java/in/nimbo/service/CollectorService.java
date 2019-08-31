package in.nimbo.service;

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
    private boolean extractKeyword;

    public CollectorService(HBaseDAO hBaseDAO, ElasticDAO elasticDAO, boolean extractKeyword) {
        this.hBaseDAO = hBaseDAO;
        this.elasticDAO = elasticDAO;
        this.extractKeyword = extractKeyword;
    }

    public boolean processList(List<Page> bufferList) {
        List<Page> filtered = bufferList.stream().filter(page -> !page.getAnchors().isEmpty()).collect(Collectors.toList());
        try {
            logger.info("Start adding {} pages to HBase", filtered.size());
            hBaseDAO.add(filtered, extractKeyword);
            logger.info("Finish adding {} pages to HBase", filtered.size());
            logger.info("Start adding {} pages to Elasticsearch", filtered.size());
            for (Page page : bufferList) {
                elasticDAO.save(page);
            }
            logger.info("Finish adding {} pages to Elasticsearch", filtered.size());
            return true;
        } catch (HBaseException | ElasticException e) {
            logger.error("Unable to establish connection", e);
            logger.info("Retry link again because of exception");
        }
        return false;
    }
}
