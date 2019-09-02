package in.nimbo.dao.elastic;

import in.nimbo.entity.Page;

import java.util.List;

public interface ElasticDAO {
    List<Page> search(String query, String site);

    List<Page> exactSearch(String query);

    List<Page> fuzzySearch(String query);
}
