package in.nimbo.common.dao.elastic;

import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.entity.Meta;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.ElasticException;
import in.nimbo.common.utility.LinkUtility;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ElasticDAOImpl implements ElasticDAO {
    private ElasticConfig config;
    private BulkProcessor bulkProcessor;
    private List<Page> backupPages;
    private RestHighLevelClient client;

    private ElasticDAOImpl(ElasticConfig config, RestHighLevelClient client, BulkProcessor bulkProcessor, List<Page> backupPages) {
        this.config = config;
        this.client = client;
        this.bulkProcessor = bulkProcessor;
        this.backupPages = backupPages;
    }

    public static ElasticDAOImpl createElasticDAO(ElasticConfig elasticConfig, CopyOnWriteArrayList<Page> backupPages) {
        ElasticBulkListener elasticBulkListener = new ElasticBulkListener(backupPages);
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(elasticConfig.getHost(), elasticConfig.getPort()))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(elasticConfig.getConnectTimeout())
                        .setSocketTimeout(elasticConfig.getSocketTimeout()))
                .setMaxRetryTimeoutMillis(elasticConfig.getMaxRetryTimeoutMillis());
        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);

        BulkProcessor.Builder builder = BulkProcessor.builder(
                (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                elasticBulkListener);
        builder.setBulkActions(elasticConfig.getBulkActions());
        builder.setBulkSize(new ByteSizeValue(elasticConfig.getBulkSize(), ByteSizeUnit.valueOf(elasticConfig.getBulkSizeUnit())));
        builder.setConcurrentRequests(elasticConfig.getConcurrentRequests());
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(elasticConfig.getBackoffDelaySeconds()),
                elasticConfig.getBackoffMaxRetry()));
        BulkProcessor bulkProcessor = builder.build();
        ElasticDAOImpl elasticDAO = new ElasticDAOImpl(elasticConfig, client, bulkProcessor, backupPages);
        elasticBulkListener.setElasticDAO(elasticDAO);
        return elasticDAO;
    }

    @Override
    public void save(Page page, double label, boolean isBulk) {
        try {
            XContentBuilder pageJson = createJSON(page, label);
            String hashedLink = LinkUtility.hashLink(page.getLink());
            IndexRequest indexRequest = new IndexRequest(config.getIndexName(), config.getType(), hashedLink)
                    .source(pageJson);
            UpdateRequest updateRequest = new UpdateRequest(config.getIndexName(), config.getType(), hashedLink)
                    .doc(pageJson)
                    .upsert(indexRequest);

            if (isBulk) {
                backupPages.add(page);
                bulkProcessor.add(updateRequest);
            } else {
                client.update(updateRequest, RequestOptions.DEFAULT);
            }
        } catch (IOException e) {
            throw new ElasticException("Save a page in ElasticSearch failed", e);
        }
    }

    @Override
    public void save(Page page, boolean isBulk) {
        save(page, -1, isBulk);
    }

    @Override
    public long count() {
        try {
            CountRequest countRequest = new CountRequest(config.getIndexName());
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            countRequest.source(searchSourceBuilder);
            CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
            return countResponse.getCount();
        } catch (IOException e) {
            throw new ElasticException("Unable to get count of search hits", e);
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public BulkProcessor getBulkProcessor() {
        return bulkProcessor;
    }

    private XContentBuilder createJSON(Page page, double label) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("link", page.getLink());
        builder.field("link_depth", page.getLinkDepth());
        builder.field("title", page.getTitle());
        builder.field("content", page.getContent());
        builder.field("forward_count", page.getAnchors().size());
        if (label >= 0) {
            builder.field("label", label);
        }
        List<Meta> metas = page.getMetas();
        builder.startArray("meta");
        for (Meta meta : metas) {
            builder.startObject();
            builder.field("key", meta.getKey());
            builder.field("content", meta.getContent());
            builder.endObject();
        }
        builder.endArray();
        builder.field("rank", page.getRank());
        builder.endObject();
        return builder;
    }
}
