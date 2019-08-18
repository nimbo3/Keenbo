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

import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.Optional;

public class CrawlerService {
    private final Timer getPageTimer;
    private final Counter skippedLinksCounter;
    private final Counter crawledLinksCounter;
    private final Counter cacheMissCounter;
    private final Counter cacheHitCounter;
    private final Timer redisContainTimer;

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
        LocalDateTime start = LocalDateTime.now();
        try {
            String siteDomain = LinkUtility.getMainDomain(siteLink);
            if (cache.getIfPresent(siteDomain) == null) {
                cacheMissCounter.inc();
                Timer.Context redisContainTimerContext = redisContainTimer.time();
                boolean contains = redisDAO.contains(siteLink);
                redisContainTimerContext.stop();
                if (!contains) {
                    crawledLinksCounter.inc();
                    redisDAO.add(siteLink);
                    cache.put(siteDomain, LocalDateTime.now());
                    Timer.Context context = getPageTimer.time();
                    Page page = parserService.getPage(siteLink);
                    context.stop();
                    return Optional.of(page);
                } else {
                    skippedLinksCounter.inc();
                    throw new InvalidLinkException("duplicated link: " + siteLink);
                }
            } else {
                cacheHitCounter.inc();
                return Optional.empty();
            }
        } catch (URISyntaxException e) {
            parserLogger.warn("Illegal URL format: " + siteLink, e);
        } catch (Exception e) {
            appLogger.error(e.getMessage(), e);
        }
        throw new InvalidLinkException();
    }
}
