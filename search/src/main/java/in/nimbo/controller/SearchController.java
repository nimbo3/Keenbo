package in.nimbo.controller;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import in.nimbo.common.utility.FileUtility;
import in.nimbo.config.SparkConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.redis.LabelDAO;
import in.nimbo.entity.*;

import java.io.IOException;
import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class SearchController {
    private static final String SEARCH_QUERY_REGEX = "^(.*?)(\\s+site:(.*))?$";
    private ElasticDAO elasticDAO;
    private LabelDAO labelDAO;

    public SearchController(ElasticDAO elasticDAO, LabelDAO labelDAO) {
        this.elasticDAO = elasticDAO;
        this.labelDAO = labelDAO;
    }

    public List<Page> search(String query, SearchType type) {
        switch (type) {
            case FUZZY:
                return elasticDAO.exactSearch(query);
            case EXACT:
                return elasticDAO.exactSearch(query);
            default:
                Pattern pattern = Pattern.compile(SEARCH_QUERY_REGEX);
                Matcher matcher = pattern.matcher(query);
                if (matcher.find()) {
                    String extractedQuery = matcher.group(1);
                    String site = matcher.group(3);
                    if (site == null) {
                        site = "";
                    }
                    List<Page> pages = elasticDAO.search(extractedQuery, site);
                    pages.forEach(page -> labelDAO.add(page.getLink(), page.getLabel()));
                    return pages;
                }
                throw new AssertionError();
        }
    }
}
