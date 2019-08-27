package in.nimbo.dao;

import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.ElasticException;
import in.nimbo.common.utility.LinkUtility;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

public class ElasticDAOImpl implements ElasticDAO {
    private ElasticConfig config;
    private List<Page> backupPages;
    private BulkProcessor bulkProcessor;

    public ElasticDAOImpl(ElasticConfig config, List<Page> backupPages, BulkProcessor bulkProcessor) {
        this.config = config;
        this.backupPages = backupPages;
        this.bulkProcessor = bulkProcessor;
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
            backupPages.add(page);
            bulkProcessor.add(updateRequest);
        } catch (IOException e) {
            throw new ElasticException("Save a page in ElasticSearch failed", e);
        }
    }
}
