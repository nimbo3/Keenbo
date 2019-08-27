package in.nimbo.dao;

import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.ElasticException;
import in.nimbo.common.utility.LinkUtility;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class ElasticDAOImpl implements ElasticDAO {
    private ElasticConfig config;
    private RestHighLevelClient client;

    public ElasticDAOImpl(ElasticConfig config, RestHighLevelClient client) {
        this.config = config;
        this.client = client;
    }

    @Override
    public void save(Page page, int label) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("link", page.getLink());
            builder.field("content", page.getContent());
            builder.field("label", label);
            builder.endObject();
            String hashedLink = LinkUtility.hashLink(page.getLink());
            IndexRequest indexRequest = new IndexRequest(config.getTestIndexName(), config.getType(), hashedLink)
                    .source(builder);
            UpdateRequest updateRequest = new UpdateRequest(config.getTestIndexName(), config.getType(), hashedLink)
                    .doc(builder)
                    .upsert(indexRequest);
            client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticException("Save a page in ElasticSearch failed", e);
        }
    }
}
