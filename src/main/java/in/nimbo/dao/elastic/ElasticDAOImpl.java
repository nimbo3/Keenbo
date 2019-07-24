package in.nimbo.dao.elastic;

import in.nimbo.config.ElasticConfig;
import in.nimbo.entity.Page;
import in.nimbo.exception.ElasticException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ElasticDAOImpl implements ElasticDAO {
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
                    .id(page.getLink())
                    .type(config.getType());

            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("link", page.getLink());
            builder.field("title", page.getTitle());
            builder.field("content", page.getContentWithoutTags());
//            builder.field("meta", page.getMetas());
            builder.field("rank", page.getRank());
            builder.endObject();
            request.source(builder);
            IndexResponse index = client.index(request, RequestOptions.DEFAULT);
            if (index.getResult() != DocWriteResponse.Result.CREATED)
                throw new ElasticException("Indexing failed!");
        } catch (IOException e) {
            throw new ElasticException("Indexing failed!", e);
        }
    }

    /**
     * get a page with given link from link from elastic search
     *
     * @param link link of page
     * @return page if it is available at database otherwise return Optional.empty
     * @throws ElasticException if any exception during indexing happen
     */
    @Override
    public Optional<Page> get(String link) {
        try {
            GetRequest getRequest = new GetRequest(config.getIndexName(), config.getType(), link);
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            if (response.isExists()) {
                Page page = new Page();
                Map<String, Object> source = response.getSource();
                if (source.containsKey("link")) {
                    page.setLink((String) source.get("link"));
                }
                if (source.containsKey("title")) {
                    page.setTitle((String) source.get("title"));
                }
                if (source.containsKey("content"))
                    page.setContentWithoutTags((String) source.get("content"));
                if (source.containsKey("rank"))
                    page.setRank((double) source.get("rank"));
                return Optional.of(page);
            } else {
                return Optional.empty();
            }
        } catch (IOException e) {
            throw new ElasticException("Unable to get page: " + link, e);
        } catch (ClassCastException e) {
            throw new ElasticException("Illegal mapping for a field", e);
        }
    }

    /**
     * @return all pages in elastic search
     * @throws ElasticException if any exception during indexing happen
     */
    @Override
    public List<Page> getAllPages() {
        try {
            ArrayList<Page> pages = new ArrayList<>();
            SearchRequest searchRequest = new SearchRequest(config.getIndexName());
            searchRequest.types(config.getType());

            SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            searchBuilder.query(QueryBuilders.matchAllQuery());
            searchBuilder.fetchSource(new String[]{"link", "title"}, new String[0]);

            searchRequest.source(searchBuilder);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                Page page = new Page();
                Map<String, DocumentField> fields = hit.getFields();
                if (fields.containsKey("link"))
                    page.setLink(fields.get("link").getValue());
                if (fields.containsKey("title"))
                    page.setTitle(fields.get("title").getValue());
                pages.add(page);
            }
            return pages;
        } catch (IOException e) {
            throw new ElasticException("Unable to get all pages", e);
        }

    }
}
