package in.nimbo.dao.elastic;

import in.nimbo.config.ElasticConfig;
import in.nimbo.entity.Page;
import in.nimbo.exception.ElasticException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticDAOImpl implements ElasticDAO {
    private final ElasticConfig config;
    private RestHighLevelClient client;

    public ElasticDAOImpl(RestHighLevelClient client, ElasticConfig config) {
        this.config = config;
        this.client = client;
    }

    private List<Page> convertHitArrayToPageList(SearchHit[] hits) {
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

    public List<Page> search(String query) {
        try {
            SearchRequest request = new SearchRequest(config.getIndexName());
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(query, "title", "link", "content", "meta", "anchors");
            multiMatchQueryBuilder.field("title", 5);
            multiMatchQueryBuilder.field("content", 1);
            multiMatchQueryBuilder.field("meta", 2);
            multiMatchQueryBuilder.field("anchors", 3);
            multiMatchQueryBuilder.field("link", 4);
            multiMatchQueryBuilder.operator(Operator.AND);
            searchSourceBuilder.query(multiMatchQueryBuilder);
            String[] includes = new String[]{"title", "link"};
            searchSourceBuilder.fetchSource(includes, null);
            request.source(searchSourceBuilder);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            return convertHitArrayToPageList(hits);
        } catch (IOException e) {
            throw new ElasticException("Unable to search in elastic search", e);
        }
    }
}
