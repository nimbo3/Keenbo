package in.nimbo.dao.hbase;

import in.nimbo.entity.Page;
import in.nimbo.exception.HBaseException;

import java.io.IOException;

public interface HBaseDAO {
    boolean contains(String link);

    void add(Page page);
}
