package in.nimbo.dao.hbase;

public interface HBaseDAO {
    void add(String link);

    boolean contains(String link);
}
