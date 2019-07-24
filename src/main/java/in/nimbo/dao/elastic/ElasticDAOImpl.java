package in.nimbo.dao.elastic;

import in.nimbo.config.ElasticConfig;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import in.nimbo.exception.ElasticException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticDAOImpl implements ElasticDAO {
    private Logger logger = LoggerFactory.getLogger(ElasticDAOImpl.class);
    private final ElasticConfig config;
    private RestHighLevelClient client;

    public ElasticDAOImpl(RestHighLevelClient client, ElasticConfig config) {
        this.config = config;
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
                    .type(config.getType());

            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("link", page.getLink());
            builder.field("title", page.getTitle());
            builder.field("content", page.getContentWithoutTags());
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
            IndexResponse index = client.index(request, RequestOptions.DEFAULT);
            if (index.getResult() != DocWriteResponse.Result.CREATED)
                logger.error("Indexing failed: " + index.getResult());
        } catch (IOException e) {
            throw new ElasticException("Indexing failed!", e);
        }
    }

    /**
     * @return all pages in elastic search
     * @throws ElasticException if any exception during indexing happen
     */
    @Override
    public List<Page> getAllPages() {
        try {
            SearchRequest searchRequest = new SearchRequest(config.getIndexName());
            searchRequest.types(config.getType());

            SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.query(QueryBuilders.matchAllQuery());
            searchBuilder.fetchSource(new String[]{"link", "title"}, new String[0]);

            searchRequest.source(searchBuilder);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            return convertHitArrayToPageList(hits);
        } catch (IOException e) {
            throw new ElasticException("Unable to get all pages", e);
        }

    }

    private List<Page> convertHitArrayToPageList(SearchHit[] hits){
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

    @Override
    public List<Page> search(String query) {
        try {
            SearchRequest request = new SearchRequest(config.getIndexName());
            request.types(config.getType());

            SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.query(QueryBuilders.multiMatchQuery(query));
            request.source(searchBuilder);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            return convertHitArrayToPageList(hits);
        } catch (IOException e) {
            throw new ElasticException("Unable to search in elastic search", e);
        }
    }
}
