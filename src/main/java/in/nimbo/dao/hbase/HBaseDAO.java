package in.nimbo.dao.hbase;

import in.nimbo.exception.HBaseException;

import java.io.IOException;

public interface HBaseDAO {
    void add(String link) throws HBaseException;

    boolean contains(String link) throws HBaseException;
}
