package in.nimbo.dao.redis;

public interface LabelDAO {
    void add(String url, int label);

    Integer get(String url);
}
