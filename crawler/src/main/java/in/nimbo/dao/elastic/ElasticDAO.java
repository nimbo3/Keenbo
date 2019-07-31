package in.nimbo.dao.elastic;

import in.nimbo.entity.Page;

public interface ElasticDAO {
    void save(Page page);

    long count();
}
