package in.nimbo.controller;

import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.entity.Page;

import java.util.List;

public class SearchController {
    private ElasticDAO elasticDAO;

    public SearchController(ElasticDAO elasticDAO) {
        this.elasticDAO = elasticDAO;
    }

    public List<Page> search(String query) {
        return elasticDAO.search(query);
    }

}
