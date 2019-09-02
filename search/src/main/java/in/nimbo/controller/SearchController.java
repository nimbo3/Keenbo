package in.nimbo.controller;

import in.nimbo.dao.auth.AuthDAO;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.redis.LabelDAO;
import in.nimbo.entity.*;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SearchController {
    private static final String SEARCH_QUERY_REGEX = "^(.*?)(\\s+site:(.*))?$";
    private ElasticDAO elasticDAO;
    private LabelDAO labelDAO;
    private AuthDAO authDAO;

    public SearchController(ElasticDAO elasticDAO, LabelDAO labelDAO, AuthDAO authDAO) {
        this.elasticDAO = elasticDAO;
        this.labelDAO = labelDAO;
        this.authDAO = authDAO;
    }

    public List<Page> search(String query, SearchType type, User user) {
        Integer label = null;
        if (user != null) {
            label = authDAO.getFavoriteLabel(user.getUsername());
        }
        List<Page> pages;
        switch (type) {
            case FUZZY:
                pages = elasticDAO.fuzzySearch(query);
                break;
            case EXACT:
                pages = elasticDAO.exactSearch(query);
                break;
            default:
                Pattern pattern = Pattern.compile(SEARCH_QUERY_REGEX);
                Matcher matcher = pattern.matcher(query);
                if (matcher.find()) {
                    String extractedQuery = matcher.group(1);
                    String site = matcher.group(3);
                    if (site == null) {
                        site = "";
                    }
                    pages = elasticDAO.search(extractedQuery, site, label);
                }
                else {
                    throw new AssertionError();
                }
        }
        pages.forEach(page -> labelDAO.add(page.getLink(), page.getLabel()));
        return pages;
    }
}
