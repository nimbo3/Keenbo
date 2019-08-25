package in.nimbo.dao;

import in.nimbo.App;
import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.dao.elastic.ElasticBulkListener;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.elastic.ElasticDAOImpl;
import in.nimbo.common.entity.Meta;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.AfterClass;
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
    private static RestHighLevelClient client;
    private static List<Page> backupPages;
    private static BulkProcessor bulkProcessor;
    private static EmbeddedElastic embeddedElastic;

    @BeforeClass
    public static void init() throws IOException, InterruptedException {
        elasticConfig = ElasticConfig.load();
        elasticConfig.setHost("localhost");
        elasticConfig.setBulkActions(2);
        elasticConfig.setBulkSize(1);
        elasticConfig.setIndexName("test-index");

        embeddedElastic = EmbeddedElastic.builder()
                .withElasticVersion("6.6.2")
                .withDownloadDirectory(new File(System.getenv("$HOME") + "/Downloads"))
                .withInstallationDirectory(new File(System.getenv("$git HOME") + "/es"))
                .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9350)
                .withSetting(PopularProperties.CLUSTER_NAME, "cluster")
                .withEsJavaOpts("-Xms128m -Xmx512m")
                .withIndex(elasticConfig.getIndexName())
                .withStartTimeout(3, TimeUnit.MINUTES)
                .build()
                .start();

        backupPages = new ArrayList<>();
        client = App.initializeElasticSearchClient(elasticConfig);
        ElasticBulkListener elasticBulkListener = new ElasticBulkListener(backupPages);
        try {
            BulkProcessor.Builder builder = BulkProcessor.builder(
                    (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                    elasticBulkListener);
            builder.setBulkActions(elasticConfig.getBulkActions());
            builder.setBulkSize(new ByteSizeValue(elasticConfig.getBulkSize(), ByteSizeUnit.valueOf(elasticConfig.getBulkSizeUnit())));
            builder.setConcurrentRequests(elasticConfig.getConcurrentRequests());
            builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(elasticConfig.getBackoffDelaySeconds()),
                    elasticConfig.getBackoffMaxRetry()));
            bulkProcessor = builder.build();
        } catch (Throwable e) {
            System.out.println(client);
            e.printStackTrace();
        }
        elasticDAO = new ElasticDAOImpl(elasticConfig, bulkProcessor, backupPages, client);
        elasticBulkListener.setElasticDAO(elasticDAO);
    }

    @AfterClass
    public static void stop() {
        embeddedElastic.stop();
    }

    @Test
    public void addTest() throws IOException, InterruptedException {
        List<Meta> metas = new ArrayList<>();
        metas.add(new Meta("description", "What"));
        Page page1 = new Page("http://aminborjian.com", "AminBorjian", "content 1", new HashSet<>(), metas, 2);
        Page page2 = new Page("http://alireza.com", "Alireza", "content 1", new HashSet<>(), metas, 1);

        elasticDAO.save(page1);
        assertEquals(page1.getLink(), backupPages.get(0).getLink());
        assertEquals(0, elasticDAO.count());

        elasticDAO.save(page2);
        assertTrue(backupPages.isEmpty());
        TimeUnit.SECONDS.sleep(1);
        assertEquals(2, elasticDAO.count());
    }
}
