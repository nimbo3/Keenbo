package in.nimbo.dao;

import in.nimbo.common.entity.Page;

public interface ElasticDAO {
    void save(Page page, String label);
}
