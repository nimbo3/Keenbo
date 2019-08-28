package in.nimbo.dao.hbase;

import in.nimbo.common.exception.HBaseException;
import in.nimbo.common.entity.Page;

import java.io.IOException;
import java.util.List;

public interface HBaseDAO extends AutoCloseable {
    @Override
    void close() throws IOException;

    /**
     * add a new page to HBase database
     * @param page page which is added
     * @throws HBaseException if any error happen during adding page
     */
    void add(List<Page> page);

    boolean contains(String link);
}
