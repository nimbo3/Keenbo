package in.nimbo.dao.hbase;

import in.nimbo.exception.HBaseException;

import java.io.IOException;

public interface HBaseDAO {
    void add(String link);

    boolean contains(String link);
}
