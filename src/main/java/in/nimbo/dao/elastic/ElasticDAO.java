package in.nimbo.dao.elastic;

import java.util.List;

public interface ElasticDAO {
    void save(String link, String text);

    void get(String link);

    List<String> getAllLinks();
}
