package in.nimbo.dao;

import in.nimbo.config.ElasticConfig;
import in.nimbo.dao.elastic.ElasticBulkListener;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.elastic.ElasticDAOImpl;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ElasticDAOTest {
    private static ElasticConfig elasticConfig;
    private static ElasticDAO elasticDAO;
    private static List<Page> backupPages;
    private static List<Page> bulkPages;
    private static RestHighLevelClient client;

    @BeforeClass
    public static void init() {
        elasticConfig = ElasticConfig.load();
        elasticConfig.setHost("localhost");
        elasticConfig.setBulkActions(2);
        elasticConfig.setIndexName("test-index");
        client = mock(RestHighLevelClient.class);

        BulkProcessor bulkProcessor = mock(BulkProcessor.class);
        TimeValue timeValue = mock(TimeValue.class);
        BulkRequest bulkRequest = mock(BulkRequest.class);
        BulkResponse bulkResponse = mock(BulkResponse.class);
        when(bulkResponse.hasFailures()).thenReturn(false);
        when(bulkResponse.getTook()).thenReturn(timeValue);
        when(timeValue.getMillis()).thenReturn(0L);

        backupPages = new ArrayList<>();
        bulkPages = new ArrayList<>();
        ElasticBulkListener elasticBulkListener = new ElasticBulkListener(backupPages);
        doAnswer(invocationOnMock -> {
            bulkPages.add(invocationOnMock.getArgument(0));
            if (bulkPages.size() >= elasticConfig.getBulkActions()) {
                elasticBulkListener.beforeBulk(1, bulkRequest);
                elasticBulkListener.afterBulk(1, bulkRequest, bulkResponse);
            }
            return null;
        }).when(bulkProcessor).add(any(IndexRequest.class));

        elasticDAO = new ElasticDAOImpl(elasticConfig, bulkProcessor, backupPages, client);
    }

    @Test
    public void addTest() throws IOException {
        List<Meta> metas = new ArrayList<>();
        metas.add(new Meta("description", "What"));
        Page page1 = new Page("http://aminborjian.com", "AminBorjian", "content 1", new HashSet<>(), metas, 2);
        Page page2 = new Page("http://alireza.com", "Alireza", "content 1", new HashSet<>(), metas, 1);

        assertTrue(bulkPages.isEmpty());

        elasticDAO.save(page1);
        assertEquals(page1.getLink(), backupPages.get(0).getLink());
        assertEquals(1, bulkPages.size());

        elasticDAO.save(page2);
        assertTrue(backupPages.isEmpty());
    }
}
