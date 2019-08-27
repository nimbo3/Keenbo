package in.nimbo.dao.elastic;

import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.exception.ElasticException;
import in.nimbo.entity.Page;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticDAOImpl implements ElasticDAO {
    private final ElasticConfig config;
    private RestHighLevelClient client;
    private List<String> violenceWords;

    public ElasticDAOImpl(RestHighLevelClient client, ElasticConfig config, List<String> violenceWords) {
        this.config = config;
        this.client = client;
        this.violenceWords = violenceWords;
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
            if (hit.getHighlightFields().containsKey("content")) {
                Text[] texts = hit.getHighlightFields().get("content").getFragments();
                StringBuilder content = new StringBuilder();
                for (Text text : texts) {
                    content.append(text.string()).append("\n");
                }
                page.setContent(content.toString());
            }
            pages.add(page);
        }
        return pages;
    }

    public List<Page> search(String query, String site) {
        try {
            SearchRequest request = new SearchRequest(config.getIndexName());
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            highlightContentField(searchSourceBuilder);
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (String word : violenceWords) {
                MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(word);
                boolQueryBuilder.mustNot(multiMatchQueryBuilder);
            }
            if (!site.equals("")) {
                MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(site, "link");
                boolQueryBuilder.must(multiMatchQueryBuilder);
            }
            MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(query, "title", "link", "content", "meta", "anchors");
            multiMatchQueryBuilder.field("title", 5);
            multiMatchQueryBuilder.field("content", 1);
            multiMatchQueryBuilder.field("meta", 2);
            multiMatchQueryBuilder.field("anchors", 3);
            multiMatchQueryBuilder.field("link", 4);
            multiMatchQueryBuilder.operator(Operator.AND);
            boolQueryBuilder.must(multiMatchQueryBuilder);
            searchSourceBuilder.query(boolQueryBuilder);
            String[] includes = new String[]{"title", "link", "content"};
            searchSourceBuilder.fetchSource(includes, null);
            request.source(searchSourceBuilder);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            return convertHitArrayToPageList(hits);
        } catch (IOException e) {
            throw new ElasticException("Unable to search in elastic search", e);
        }
    }

    private void highlightContentField(SearchSourceBuilder searchSourceBuilder) {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field(new HighlightBuilder.Field("content"));
        highlightBuilder.preTags("<span class=\"highlighted\">");
        highlightBuilder.postTags("</span>");
        searchSourceBuilder.highlighter(highlightBuilder);
    }
}
