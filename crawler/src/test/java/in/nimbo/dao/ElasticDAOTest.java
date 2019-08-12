package in.nimbo.dao;

import in.nimbo.App;
import in.nimbo.common.config.ElasticConfig;
import in.nimbo.dao.elastic.ElasticBulkListener;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.elastic.ElasticDAOImpl;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.BeforeClass;
import org.junit.Test;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticDAOTest {
    private static ElasticConfig elasticConfig;
    private static ElasticDAO elasticDAO;
    private static List<Page> backupPages;

    @BeforeClass
    public static void init() throws IOException, InterruptedException {
        elasticConfig = ElasticConfig.load();
        elasticConfig.setHost("localhost");
        elasticConfig.setBulkActions(2);
        elasticConfig.setIndexName("test-index");

        EmbeddedElastic embeddedElastic = EmbeddedElastic.builder()
                .withElasticVersion("6.6.2")
                .withDownloadDirectory(new File(System.getenv("$HOME") + "/Downloads"))
                .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9350)
                .withSetting(PopularProperties.CLUSTER_NAME, "cluster")
                .withEsJavaOpts("-Xms128m -Xmx512m")
                .withIndex(elasticConfig.getIndexName())
                .withStartTimeout(3, TimeUnit.MINUTES)
                .build()
                .start();

        backupPages = new ArrayList<>();
        RestHighLevelClient client = App.initializeElasticSearchClient(elasticConfig);
        ElasticBulkListener elasticBulkListener = new ElasticBulkListener(backupPages);
        BulkProcessor bulkProcessor = App.initializeElasticSearchBulk(elasticConfig, client, elasticBulkListener);
        elasticDAO = new ElasticDAOImpl(elasticConfig, bulkProcessor, backupPages, client);
        elasticBulkListener.setElasticDAO(elasticDAO);
    }

    @Test
    public void addTest() throws IOException {
        List<Meta> metas = new ArrayList<>();
        metas.add(new Meta("description", "What"));
        Page page1 = new Page("http://aminborjian.com", "AminBorjian", "content 1", new HashSet<>(), metas, 2);
        Page page2 = new Page("http://alireza.com", "Alireza", "content 1", new HashSet<>(), metas, 1);

        elasticDAO.save(page1);
        assertEquals(page1.getLink(), backupPages.get(0).getLink());
        assertEquals(0, elasticDAO.count());

        elasticDAO.save(page2);
        assertTrue(backupPages.isEmpty());
        assertEquals(2, elasticDAO.count());
    }
}
