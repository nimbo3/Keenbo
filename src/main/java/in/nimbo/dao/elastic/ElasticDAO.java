package in.nimbo.dao.elastic;

import java.util.List;
import java.util.Optional;

public interface ElasticDAO {
    void save(String link, String text);

    Optional<String> get(String link);

    List<String> getAllLinks();
}
