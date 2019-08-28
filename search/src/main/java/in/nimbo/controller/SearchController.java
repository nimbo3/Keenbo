package in.nimbo.controller;

import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.entity.Page;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SearchController {
    private static final String SEARCH_QUERY_REGEX = "^(.*?)(\\s+site:(.*))?$";
    private ElasticDAO elasticDAO;

    public SearchController(ElasticDAO elasticDAO) {
        this.elasticDAO = elasticDAO;
    }

    public List<Page> search(String query) {
        Pattern pattern = Pattern.compile(SEARCH_QUERY_REGEX);
        Matcher matcher = pattern.matcher(query);
        if (matcher.find()) {
            String extractedQuery = matcher.group(1);
            String site = matcher.group(3);
            if (site == null)
                site = "";
            return elasticDAO.search(extractedQuery, site);
        }
        throw new AssertionError();
    }

}
