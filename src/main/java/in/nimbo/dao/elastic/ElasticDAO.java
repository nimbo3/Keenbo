package in.nimbo.dao.elastic;

import in.nimbo.entity.Page;

import java.util.List;
import java.util.Optional;

public interface ElasticDAO {
    void save(Page page);

    Optional<Page> get(String link);

    List<Page> getAllPages();

    List<Page> search(String query);
}
