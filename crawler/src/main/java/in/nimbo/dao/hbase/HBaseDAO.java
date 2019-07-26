package in.nimbo.dao.hbase;

import in.nimbo.entity.Page;

import java.io.IOException;

public interface HBaseDAO extends AutoCloseable {
    @Override
    void close() throws IOException;

    boolean contains(String link);

    void add(Page page);
}
