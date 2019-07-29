package in.nimbo.dao.hbase;

import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.entity.Page;
import in.nimbo.service.ParserService;

import java.io.IOException;

public interface HBaseDAO extends AutoCloseable {
    @Override
    void close() throws IOException;

    /**
     * add a new page to HBase database
     * @param page page which is added
     * @return true if page added successfully
     * @throws in.nimbo.exception.HBaseException if any error happen during adding page
     */
    boolean add(Page page);
}
