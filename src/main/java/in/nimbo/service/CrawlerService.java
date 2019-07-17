package in.nimbo.service;

import java.util.List;

public interface CrawlerService {
    /**
     * get a link of a site
     * @param link link of site
     * @return all links of site inside that page
     */
    List<String> crawl(String link);
}
