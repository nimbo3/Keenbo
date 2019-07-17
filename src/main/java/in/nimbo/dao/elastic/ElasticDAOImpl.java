package in.nimbo.dao.elastic;

import in.nimbo.conf.ElasticConfig;
import in.nimbo.exception.ElasticException;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticDAOImpl implements ElasticDAO {
    private final String ES_INDEX;
    private RestHighLevelClient client;

    public ElasticDAOImpl(ElasticConfig config) {
        ES_INDEX = config.getIndexName();
        client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200)));
    }

    @Override
    public void save(String link, String text) throws ElasticsearchException {
        try {
            IndexRequest request = new IndexRequest(ES_INDEX).id(link);
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("text", text);
            builder.endObject();
            request.source(builder);
            IndexResponse index = client.index(request, RequestOptions.DEFAULT);
            if (index.getResult() != DocWriteResponse.Result.CREATED)
                throw new ElasticsearchException("Indexing failed!");
        } catch (IOException e) {
            throw new ElasticsearchException("Indexing failed!", e);
        }
    }

    @Override
    public String get(String link) throws ElasticException {
        try {
            GetRequest getRequest = new GetRequest(ES_INDEX, link);
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            String text = (String) response.getSource().get("text");
            if (text == null)
                throw new ElasticException("Get failed");
            return text;
        } catch (IOException | ClassCastException e) {
            throw new ElasticsearchException("Get failed", e);
        }
    }

    @Override
    public List<String> getAllLinks() throws ElasticException {
        try {
            ArrayList<String> links = new ArrayList<>();
            SearchRequest searchRequest = new SearchRequest(ES_INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchRequest.source(searchSourceBuilder);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits)
                links.add(hit.getId());
            return links;
        } catch (IOException e) {
            throw new ElasticException("Search failed", e);
        }

    }
}
