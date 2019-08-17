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
import java.time.temporal.ChronoUnit;
import java.util.Optional;

public class CrawlerService {
    private final Timer getPageTimer;
    private final Counter skippedCounter;
    private final Counter crawledCounter;
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
        skippedCounter = metricRegistry.counter(MetricRegistry.name(CrawlerService.class, "skipCounter"));
        crawledCounter = metricRegistry.counter(MetricRegistry.name(CrawlerService.class, "crawledCounter"));
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
        boolean isLinkSkipped = true;
        try {
            String siteDomain = LinkUtility.getMainDomain(siteLink);
            if (cache.getIfPresent(siteDomain) == null) {
                Timer.Context redisContainTimerContext = redisContainTimer.time();
                boolean contains = redisDAO.contains(siteLink);
                redisContainTimerContext.stop();
                if (!contains) {
                    redisDAO.add(siteLink);
                    cache.put(siteDomain, LocalDateTime.now());
                    isLinkSkipped = false;
                    Timer.Context context = getPageTimer.time();
                    Page page = parserService.getPage(siteLink);
                    context.stop();
                    return Optional.of(page);
                } else {
                    throw new InvalidLinkException("duplicated link: " + siteLink);
                }
            } else {
                return Optional.empty();
            }
        } catch (URISyntaxException e) {
            parserLogger.warn("Illegal URL format: " + siteLink, e);
        } catch (Exception e) {
            appLogger.error(e.getMessage(), e);
        } finally {
            LocalDateTime end = LocalDateTime.now();
            long duration = start.until(end, ChronoUnit.MILLIS);
            if (isLinkSkipped) {
                skippedCounter.inc(duration);
            } else {
                crawledCounter.inc(duration);
            }
        }
        throw new InvalidLinkException();
    }
}
