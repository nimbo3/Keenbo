package in.nimbo.dao;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticBulkListener implements BulkProcessor.Listener {
    private Logger logger = LoggerFactory.getLogger("collector");

    @Override
    public void beforeBulk(long executionId, BulkRequest bulkRequest) {
        int numberOfActions = bulkRequest.numberOfActions();
        logger.info("Executing bulk [{}] with {} requests", executionId, numberOfActions);
    }

    @Override
    public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {

    }

    @Override
    public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

    }
}
