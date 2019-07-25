package in.nimbo.dao.elastic;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticBulkListener implements BulkProcessor.Listener {
    private Logger logger = LoggerFactory.getLogger(ElasticBulkListener.class);

    @Override
    public void beforeBulk(long executionId, BulkRequest bulkRequest) {
        int numberOfActions = bulkRequest.numberOfActions();
        logger.info("Executing bulk [{}] with {} requests", executionId, numberOfActions);
    }

    @Override
    public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
        if (bulkResponse.hasFailures()) {
            logger.warn("Bulk [{}] executed with failures", executionId);
        } else {
            logger.debug("Bulk [{}] completed in {} milliseconds", executionId, bulkResponse.getTook().getMillis());
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
        logger.error("Failed to execute bulk {}", executionId, throwable);
    }
}
