package in.nimbo.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.common.dao.elastic.ElasticDAO;
import in.nimbo.common.entity.Link;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.InvalidLinkException;
import in.nimbo.common.service.ParserService;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.entity.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class CrawlerService {
    private Cache<String, LocalDateTime> politenessCache;
    private Cache<String, LocalDateTime> crawlerCache;
    private ParserService parserService;
    private ElasticDAO elasticDao;
    private Map<String, Double> labelMap;
    private Logger appLogger = LoggerFactory.getLogger("classifier");

    public CrawlerService(Cache<String, LocalDateTime> politenessCache, Cache<String, LocalDateTime> crawlerCache,
                          ParserService parserService, ElasticDAO elasticDao, Map<String, Double> labelMap) {
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
            boolean isPolite = politenessCache.getIfPresent(domain) == null;
            boolean isDuplicate = crawlerCache.getIfPresent(url) != null;
            if (isPolite && !isDuplicate) {
                LocalDateTime now = LocalDateTime.now();
                politenessCache.put(domain, now);
                crawlerCache.put(url, now);
                Page page = parserService.getPage(url);
                elasticDao.save(page, labelMap.get(link.getLabel()), false);
                return Optional.of(page);
            } else if (isDuplicate) {
                appLogger.info("Skip link {} because crawled before", url);
                throw new InvalidLinkException("duplicated link: " + url);
            } else {
                return Optional.empty();
            }
        } catch (MalformedURLException e) {
            appLogger.warn("Illegal URL format: " + url, e);
        }
        throw new InvalidLinkException();
    }

    public static void fillInitialCrawlQueue(BlockingQueue<Link> queue, List<Category> categories) {
        for (Category category : categories) {
            for (String site : category.getSites()) {
                try {
                    queue.put(new Link("https://" + site, category.getName(), 0));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static List<Category> loadFeed(ObjectMapper mapper) throws IOException {
        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("first-feed.json");
        Scanner scanner = new Scanner(resourceAsStream);
        StringBuilder stringBuilder = new StringBuilder("");
        while (scanner.hasNextLine()) {
            stringBuilder.append(scanner.nextLine());
        }
        String json = stringBuilder.toString();
        CollectionType collectionType = mapper.getTypeFactory().constructCollectionType(List.class, Category.class);
        return mapper.readValue(json, collectionType);
    }

    public static List<String> loadDomains(List<Category> categories) {
        List<String> domains = new ArrayList<>();
        for (Category category : categories) {
            domains.addAll(category.getSites());
        }
        return domains;
    }

    public static Map<String, Double> loadLabels(List<Category> categories) {
        Map<String, Double> map = new HashMap<>();
        double mapId = 0;
        for (Category category : categories) {
            map.put(category.getName(), mapId);
            mapId++;
        }
        return map;
    }
}
