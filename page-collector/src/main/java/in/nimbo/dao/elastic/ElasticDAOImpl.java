package in.nimbo.dao.elastic;

import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.exception.ElasticException;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.common.entity.Meta;
import in.nimbo.common.entity.Page;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;

public class ElasticDAOImpl implements ElasticDAO {
    private final ElasticConfig config;
    private BulkProcessor bulkProcessor;
    private List<Page> backupPages;
    private RestHighLevelClient client;

    public ElasticDAOImpl(ElasticConfig config, BulkProcessor bulkProcessor, List<Page> backupPages, RestHighLevelClient client) {
        this.config = config;
        this.bulkProcessor = bulkProcessor;
        this.backupPages = backupPages;
        this.client = client;
    }

    @Override
    public void save(Page page) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("link", page.getLink());
            builder.field("link_depth", page.getLinkDepth());
            builder.field("title", page.getTitle());
            builder.field("content", page.getContent());
            builder.field("forward_count", page.getAnchors().size());
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

            String hashedLink = LinkUtility.hashLink(page.getLink());
            IndexRequest indexRequest = new IndexRequest(config.getIndexName(), config.getType(), hashedLink)
                    .source(builder);
            UpdateRequest updateRequest = new UpdateRequest(config.getIndexName(), config.getType(), hashedLink)
                    .doc(builder)
                    .upsert(indexRequest);

            backupPages.add(page);
            bulkProcessor.add(updateRequest);
        } catch (IOException e) {
            throw new ElasticException("Save a page in ElasticSearch failed", e);
        }
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
}
