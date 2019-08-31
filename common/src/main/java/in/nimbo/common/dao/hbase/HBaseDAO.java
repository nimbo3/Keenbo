package in.nimbo.common.dao.hbase;

import in.nimbo.common.entity.Page;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface HBaseDAO extends AutoCloseable {
    @Override
    void close() throws IOException;

    void add(List<Page> pages);

    void add(List<Page> pages, List<Map<String, Integer>> keywords);

    boolean contains(String link);
}
