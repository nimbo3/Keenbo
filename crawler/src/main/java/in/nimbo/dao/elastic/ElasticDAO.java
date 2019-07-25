package in.nimbo.dao.elastic;

import in.nimbo.entity.Page;

import java.util.List;
import java.util.Optional;

public interface ElasticDAO {
    void save(Page page);

    List<Page> getAllPages();

    List<Page> search(String query);
}
