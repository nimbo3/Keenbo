package in.nimbo.dao;

import in.nimbo.App;
import in.nimbo.config.ElasticConfig;
import in.nimbo.dao.elastic.ElasticBulkListener;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.elastic.ElasticDAOImpl;
import in.nimbo.entity.Page;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.IndexSettings;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticDAOTest {
    private static ElasticConfig elasticConfig;
    private static RestHighLevelClient restHighLevelClient;
    private static BulkProcessor bulkProcessor;
    private static ElasticDAO elasticDAO;
    private static List<Page> backupList;

    @BeforeClass
    public static void init() throws IOException, InterruptedException {
        InputStream resourceAsStream = ElasticDAOTest.class.getClassLoader().getResourceAsStream("elastic/elastic-mapping.json");
        Assert.assertNotNull(resourceAsStream);
        elasticConfig = ElasticConfig.load();
        elasticConfig.setBulkActions(2);
        EmbeddedElastic embeddedElastic = EmbeddedElastic.builder()
                .withElasticVersion("6.6.2")
                .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9350)
                .withSetting(PopularProperties.CLUSTER_NAME, "KeenboTest")
                .withEsJavaOpts("-Xms128m -Xmx512m")
                .withPlugin("analysis-stempel")
                .withIndex(elasticConfig.getIndexName(), IndexSettings.builder()
                        .withType(elasticConfig.getType(), resourceAsStream)
                        .build())
                .build()
                .start();
        List<Page> backupList = new ArrayList<>();
        restHighLevelClient = App.initializeElasticSearchClient(elasticConfig);
        bulkProcessor = App.initializeElasticSearchBulk(elasticConfig, restHighLevelClient,
                new ElasticBulkListener(backupList));
        elasticDAO = new ElasticDAOImpl(elasticConfig, bulkProcessor, backupList);
    }

    @Before
    public void beforeEachTest() {

    }

    private List<Page> getPages() throws IOException {
        SearchRequest request = new SearchRequest(elasticConfig.getIndexName());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(matchAllQueryBuilder);
        request.source(searchSourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        List<Page> pages = new ArrayList<>();
        for (SearchHit hit : hits) {
            Page page = new Page();
            Map<String, Object> fields = hit.getSourceAsMap();
            if (fields.containsKey("link")) {
                page.setLink((String) fields.get("link"));
            }
            if (fields.containsKey("title")) {
                page.setTitle((String) fields.get("title"));
            }
            pages.add(page);
        }
        return pages;
    }

    @Test
    public void addTest() throws IOException {
        Page page1 = new Page("http://stackoverflow.com", "StackOverflow", "content 1", new HashSet<>(), new ArrayList<>(), 2);
        Page page2 = new Page("http://google.com", "Google", "content 1", new HashSet<>(), new ArrayList<>(), 1);

        List<Page> pages = getPages();
        assertTrue(pages.isEmpty());

        elasticDAO.save(page1);
        assertEquals(page1.getLink(), backupList.get(0).getLink());
        pages = getPages();
        assertEquals(1, pages.size());
        assertEquals(page1.getLink(), pages.get(0).getLink());

        elasticDAO.save(page2);
        assertEquals(page1.getLink(), backupList.get(0).getLink());
        assertEquals(page2.getLink(), backupList.get(1).getLink());
        assertEquals(2, pages.size());
        assertTrue(pages.contains(page1));
        assertTrue(pages.contains(page2));
    }
}
