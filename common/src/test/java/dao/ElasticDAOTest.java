package dao;

import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.dao.elastic.ElasticDAOImpl;
import in.nimbo.common.entity.Meta;
import in.nimbo.common.entity.Page;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticDAOTest {
    private static ElasticDAOImpl elasticDAO;
    private static CopyOnWriteArrayList<Page> backupPages;

    @BeforeClass
    public static void init() throws IOException {
        ElasticConfig elasticConfig = ElasticConfig.load();
        elasticConfig.setHost("localhost");
        elasticConfig.setBulkActions(2);
        elasticConfig.setBulkSize(1);
        elasticConfig.setIndexName("test-index");

        backupPages = new CopyOnWriteArrayList<>();
        elasticDAO = ElasticDAOImpl.createElasticDAO(elasticConfig, backupPages);


        CreateIndexRequest request = new CreateIndexRequest(elasticConfig.getIndexName());
        elasticDAO.getClient().indices().create(request, RequestOptions.DEFAULT);
    }

    @AfterClass
    public static void stop() throws IOException {
        elasticDAO.close();
    }

    @Test
    public void addTest() throws IOException, InterruptedException {
        List<Meta> metas = new ArrayList<>();
        metas.add(new Meta("description", "What"));
        Page page1 = new Page("http://aminborjian.com", "AminBorjian", "content 1", new HashSet<>(), metas, 2);
        Page page2 = new Page("http://alireza.com", "Alireza", "content 1", new HashSet<>(), metas, 1);

        elasticDAO.save(page1, 1.0, true);
        assertEquals(page1.getLink(), backupPages.get(0).getLink());
        assertEquals(0, elasticDAO.count());

        elasticDAO.save(page2, 1.0, true);
        assertTrue(backupPages.isEmpty());
        TimeUnit.SECONDS.sleep(1);
        assertEquals(2, elasticDAO.count());
    }
}
