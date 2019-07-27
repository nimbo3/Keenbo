package in.nimbo.dao.elastic;

import in.nimbo.config.ElasticConfig;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import in.nimbo.exception.ElasticException;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

public class ElasticDAOImpl implements ElasticDAO {
    private final ElasticConfig config;
    private BulkProcessor bulkProcessor;
    private List<Page> backupPages;

    public ElasticDAOImpl(ElasticConfig config, BulkProcessor bulkProcessor, List<Page> backupPages) {
        this.config = config;
        this.bulkProcessor = bulkProcessor;
        this.backupPages = backupPages;
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
            bulkProcessor.add(request);
            backupPages.add(page);
        } catch (IOException e) {
            throw new ElasticException("Save a page in ElasticSearch failed", e);
        }
    }
}
