package in.nimbo.dao.elastic;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkListener implements BulkProcessor.Listener {
    private Logger logger = LoggerFactory.getLogger(BulkListener.class);

    @Override
    public void beforeBulk(long l, BulkRequest bulkRequest) {
        int numberOfActions = request.numberOfActions();
        logger.debug("Executing bulk [{}] with {} requests",
                executionId, numberOfActions);
    }

    @Override
    public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
        if (response.hasFailures()) {
            logger.warn("Bulk [{}] executed with failures", executionId);
        } else {
            logger.debug("Bulk [{}] completed in {} milliseconds",
                    executionId, response.getTook().getMillis());
        }
    }

    @Override
    public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
        logger.error("Failed to execute bulk", failure);

    }
}
