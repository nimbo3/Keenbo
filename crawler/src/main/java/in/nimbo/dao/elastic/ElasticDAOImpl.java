package in.nimbo.dao.elastic;

import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.exception.ElasticException;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import in.nimbo.common.utility.LinkUtility;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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

    /**
     * save necessary page field in elastic search
     *
     * @param page page
     * @throws ElasticException if any exception during indexing happen
     */
    @Override
    public void save(Page page) {
        try {
            IndexRequest request = new IndexRequest(config.getIndexName())
                    .type(config.getType())
                    .id(LinkUtility.hashLink(page.getLink()));

            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("link", page.getLink());
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
            request.source(builder);
            backupPages.add(page);
            bulkProcessor.add(request);
        } catch (IOException e) {
            throw new ElasticException("Save a page in ElasticSearch failed", e);
        }
    }

    @Override
    public long count() {
        try {
            SearchRequest request = new SearchRequest(config.getIndexName());
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);
            request.source(searchSourceBuilder);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return response.getHits().getTotalHits();
        } catch (IOException e) {
            throw new ElasticException("Unable to get count of search hits", e);
        }
    }
}
