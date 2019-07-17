package in.nimbo.service;

import java.util.List;

public interface CrawlerService {
    /**
     * get a link of a site
     * @param link link of site
     * @return all links of site inside that page
     */
    List<String> crawl(String link);

    /**
     * check whether a link is cached or not
     * @param link link of a site which it's domain is checked
     * @return true if domain is cached
     */
    boolean isCached(String link);
}
