package in.nimbo.dao.redis;

public interface RedisDAO {
    void add(String link);

    boolean contains(String link);
}
