package in.nimbo.common.dao.elastic;

import in.nimbo.common.entity.Page;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ElasticBulkListener implements BulkProcessor.Listener {
    private Logger logger = LoggerFactory.getLogger("collector");
    private ElasticDAO elasticDAO;
    private List<Page> backupPages;

    public ElasticBulkListener(List<Page> backupPages) {
        this.backupPages = backupPages;
    }

    public void setElasticDAO(ElasticDAO elasticDAO) {
        this.elasticDAO = elasticDAO;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest bulkRequest) {
        int numberOfActions = bulkRequest.numberOfActions();
        logger.info("Executing bulk [{}] with {} requests", executionId, numberOfActions);
    }

    @Override
    public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
        if (bulkResponse.hasFailures()) {
            long failCount = 0;
            for (BulkItemResponse item : bulkResponse.getItems()) {
                if (item.isFailed()) {
                    try {
                        Page failedPage = backupPages.get(item.getItemId());
                        logger.info("Failed to index page: {}", failedPage.getLink());
                        logger.info("Retry page: {}", failedPage.getLink());
                        failCount++;
                    } catch (IndexOutOfBoundsException e) {
                        logger.info("Unable to find page with index {} inside {} requests", item.getItemId(), bulkRequest.numberOfActions());
                    }
                }
            }
            logger.error("Bulk [{}] executed with {} failures", executionId, failCount);
        } else {
            logger.info("Bulk [{}] completed in {} milliseconds", executionId, bulkResponse.getTook().getMillis());
            backupPages.clear();
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
        if (!(throwable instanceof java.lang.InterruptedException)) {
            logger.error("Failed to execute bulk {}", executionId, throwable);
            for (Page page : backupPages) {
                elasticDAO.save(page, true);
            }
            logger.info("Retry all {} requests again", bulkRequest.numberOfActions());
        }
    }
}
