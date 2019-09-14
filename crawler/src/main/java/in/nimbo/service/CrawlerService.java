package in.nimbo.service;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Meta;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.InvalidLinkException;
import in.nimbo.common.exception.LanguageDetectException;
import in.nimbo.common.exception.ParseLinkException;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.dao.redis.RedisDAO;
import org.jsoup.Connection;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
                    Page page = getPage(siteLink);
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

    /**
     * crawl a site and return it's content as a page
     *
     * @param link link of site
     * @return page if able to crawl page
     */
    public Page getPage(String link) {
        try {
            Connection.Response response = parserService.getResponse(link);
            String redirectedLink = response.url().toExternalForm();
            if (isCrawled(redirectedLink)) {
                throw new ParseLinkException("Url crawled after follow redirects: " + link);
            }
            redisDAO.add(LinkUtility.hashLinkCompressed(redirectedLink));
            cache.put(LinkUtility.getMainDomain(redirectedLink), LocalDateTime.now());
            Document document = parserService.getDocument(response);
            String pageContentWithoutTag = document.text().replace("\n", " ");
            if (pageContentWithoutTag.isEmpty()) {
                parserLogger.warn("There is no content for site: {}", link);
            } else if (parserService.isEnglishLanguage(pageContentWithoutTag)) {
                Set<Anchor> anchors = parserService.getAnchors(document);
                List<Meta> metas = parserService.getMetas(document);
                String title = parserService.getTitle(document);
                if (title.isEmpty()) {
                    title = link;
                }
                return new Page(redirectedLink, title, pageContentWithoutTag, anchors, metas, 1.0);
            }
        } catch (MalformedURLException e) {
            appLogger.warn("Unable to reverse link: {}", link);
        } catch (LanguageDetectException e) {
            parserLogger.warn("Cannot detect language of site: {}", link);
        }
        throw new ParseLinkException();
    }

    public boolean isCrawled(String link) {
        return redisDAO.contains(LinkUtility.hashLinkCompressed(link));
    }
}
