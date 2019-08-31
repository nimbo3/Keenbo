package in.nimbo.common.dao.elastic;


import in.nimbo.common.entity.Page;

import java.io.IOException;

public interface ElasticDAO extends AutoCloseable {
    void save(Page page, double label, boolean isBulk);

    void save(Page page, boolean isBulk);

    long count();

    void close() throws IOException;
}
