package in.nimbo.service;

import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.InvalidLinkException;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.dao.ElasticDAO;
import in.nimbo.entity.Link;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

public class CrawlerService {
    private Cache<String, LocalDateTime> politenessCache;
    private Cache<String, LocalDateTime> crawlerCache;
    private ParserService parserService;
    private ElasticDAO elasticDao;
    private Map<String, Integer> labelMap;
    private Logger appLogger = LoggerFactory.getLogger("app");

    public CrawlerService(Cache<String, LocalDateTime> politenessCache, Cache<String, LocalDateTime> crawlerCache, ParserService parserService, ElasticDAO elasticDao, Map<String, Integer> labelMap) {
        this.politenessCache = politenessCache;
        this.crawlerCache = crawlerCache;
        this.parserService = parserService;
        this.elasticDao = elasticDao;
        this.labelMap = labelMap;
    }

    public Optional<Page> crawl(Link link) throws MalformedURLException {
        String url = LinkUtility.normalize(link.getUrl());
        try {
            String domain = LinkUtility.getMainDomain(url);
            boolean politeness = politenessCache.getIfPresent(domain) == null;
            boolean duplicate = crawlerCache.getIfPresent(url) == null;
            if (politeness && duplicate) {
                LocalDateTime now = LocalDateTime.now();
                politenessCache.put(domain, now);
                crawlerCache.put(url, now);
                System.out.println(url);
                Page page = parserService.getPage(url);
                elasticDao.save(page, labelMap.get(link.getLabel()));
                return Optional.of(page);
            }
            else if (!duplicate){
                appLogger.info("Skip link {} because crawled before", url);
                throw new InvalidLinkException("duplicated link: " + url);
            }
            else {
                return Optional.empty();
            }
        } catch (MalformedURLException e) {
            appLogger.warn("Illegal URL format: " + url, e);
        }
        throw new InvalidLinkException();
    }
}
