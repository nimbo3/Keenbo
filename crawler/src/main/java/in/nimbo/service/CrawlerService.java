package in.nimbo.service;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.InvalidLinkException;
import in.nimbo.common.exception.ParseLinkException;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.dao.redis.RedisDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.time.LocalDateTime;
import java.util.Optional;

public class CrawlerService {
    private Timer getPageTimer;
    private Counter skippedLinksCounter;
    private Counter crawledLinksCounter;
    private Counter cacheMissCounter;
    private Counter cacheHitCounter;
    private Timer redisContainTimer;

    private Logger appLogger = LoggerFactory.getLogger("app");
    private Logger parserLogger = LoggerFactory.getLogger("parser");
    private Cache<String, LocalDateTime> cache;
    private RedisDAO redisDAO;
    private ParserService parserService;

    public CrawlerService(Cache<String, LocalDateTime> cache,
                          RedisDAO redisDAO,
                          ParserService parserService) {
        this.cache = cache;
        this.parserService = parserService;
        this.redisDAO = redisDAO;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        getPageTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "getPage"));
        skippedLinksCounter = metricRegistry.counter(MetricRegistry.name(CrawlerService.class, "skippedLinksCounter"));
        crawledLinksCounter = metricRegistry.counter(MetricRegistry.name(CrawlerService.class, "crawledLinksCounter"));
        cacheMissCounter = metricRegistry.counter(MetricRegistry.name(CrawlerService.class, "cacheMissCounter"));
        cacheHitCounter = metricRegistry.counter(MetricRegistry.name(CrawlerService.class, "cacheHitCounter"));
        redisContainTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "redisContain"));
    }

    /**
     *
     * @param siteLink url of crawling page
     * @return crawler page otherwise Optional.empty if this domain was visited recently
     * @throws InvalidLinkException if link is invalid
     * @throws ParseLinkException if any exception happen in parser
     */
    public Optional<Page> crawl(String siteLink) {
        appLogger.info("Start crawling link {}", siteLink);
        try {
            String siteDomain = LinkUtility.getMainDomain(siteLink);
            if (cache.getIfPresent(siteDomain) == null) {
                cacheMissCounter.inc();
                Timer.Context redisContainTimerContext = redisContainTimer.time();
                boolean contains = isCrawled(siteLink);
                redisContainTimerContext.stop();
                if (!contains) {
                    crawledLinksCounter.inc();
                    redisDAO.add(LinkUtility.hashLinkCompressed(siteLink));
                    cache.put(siteDomain, LocalDateTime.now());
                    Timer.Context context = getPageTimer.time();
                    appLogger.info("Start parse link {}", siteLink);
                    Page page = parserService.getPage(siteLink);
                    appLogger.info("Finish parsing link {}", siteLink);
                    context.stop();
                    return Optional.of(page);
                } else {
                    appLogger.info("Skip link {} because crawled before", siteLink);
                    skippedLinksCounter.inc();
                    throw new InvalidLinkException("duplicated link: " + siteLink);
                }
            } else {
                appLogger.info("Skip link {} because of cache hit", siteLink);
                cacheHitCounter.inc();
                return Optional.empty();
            }
        } catch (MalformedURLException e) {
            appLogger.warn("Illegal URL format: " + siteLink, e);
        }
        throw new InvalidLinkException();
    }

    public boolean isCrawled(String link) {
        return redisDAO.contains(LinkUtility.hashLinkCompressed(link));
    }
}
