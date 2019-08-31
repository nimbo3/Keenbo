package in.nimbo.dao.redis;

public interface LabelDAO {
    void add(String url, double label);

    Integer get(String url);
}
