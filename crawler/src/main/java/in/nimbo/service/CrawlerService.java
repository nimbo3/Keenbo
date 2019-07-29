package in.nimbo.service;

import com.codahale.metrics.*;
import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.redis.RedisDAO;
import in.nimbo.entity.Anchor;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import in.nimbo.exception.HBaseException;
import in.nimbo.exception.LanguageDetectException;
import in.nimbo.service.kafka.ConsumerService;
import in.nimbo.utility.LinkUtility;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class CrawlerService {
    private final Counter crawledPages;
    private final Timer getPageTimer;
    private final Timer redisContainsTimer;
    private final Timer elasticSaveTimer;
    private final Timer hBaseAddTimer;
    private final Timer redisAddTimer;
    private final Histogram crawledHistogram;
    private final Histogram skippedTimeHistogram;
    private final Counter skippedCounter;
    private final Counter crawledCounter;

    private Logger logger = LoggerFactory.getLogger(ConsumerService.class);
    private Cache<String, LocalDateTime> cache;
    private HBaseDAO hBaseDAO;
    private ElasticDAO elasticDAO;
    private ParserService parserService;
    private RedisDAO redisDAO;

    private AtomicLong TotalCrawl = new AtomicLong(0);
    private AtomicLong TotalSkip = new AtomicLong(0);

    public CrawlerService(Cache<String, LocalDateTime> cache,
                          HBaseDAO hBaseDAO, ElasticDAO elasticDAO,
                          ParserService parserService,
                          RedisDAO redisDAO) {
        this.cache = cache;
        this.hBaseDAO = hBaseDAO;
        this.elasticDAO = elasticDAO;
        this.parserService = parserService;
        this.redisDAO = redisDAO;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        crawledPages = metricRegistry.counter(MetricRegistry.name(CrawlerService.class, "crawledPages"));
        getPageTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "getPage"));
        redisContainsTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "redisContains"));
        elasticSaveTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "elasticSave"));
        hBaseAddTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "hBaseAdd"));
        redisAddTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "redisAdd"));
        crawledHistogram = metricRegistry.histogram(MetricRegistry.name(CrawlerService.class, "crawledTimes"));
        skippedTimeHistogram = metricRegistry.histogram(MetricRegistry.name(CrawlerService.class, "skippedTimes"));
        skippedCounter = metricRegistry.counter(MetricRegistry.name(CrawlerService.class, "skipCounter"));
        crawledCounter = metricRegistry.counter(MetricRegistry.name(CrawlerService.class, "crawledCounter"));
    }

    public Set<String> crawl(String siteLink) {
        Set<String> links = new HashSet<>();
        LocalDateTime start = LocalDateTime.now();
        boolean isLinkSkipped = true;
        try {
            String siteDomain = LinkUtility.getMainDomain(siteLink);
            if (cache.getIfPresent(siteDomain) == null) {
                isLinkSkipped = false;

                Timer.Context redisContainsTimerContext = redisContainsTimer.time();
                boolean contains = redisDAO.contains(siteLink);
                redisContainsTimerContext.stop();

                if (!contains) {
                    Optional<Page> pageOptional = getPage(siteLink);
                    cache.put(siteDomain, LocalDateTime.now());
                    if (pageOptional.isPresent()) {
                        Page page = pageOptional.get();
                        page.getAnchors().forEach(link -> links.add(link.getHref()));

                        boolean isAddedToHBase;
                        if (page.getAnchors().isEmpty()) {
                            isAddedToHBase = true;
                        } else {
                            Timer.Context hBaseAddTimerContext = hBaseAddTimer.time();
                            isAddedToHBase = hBaseDAO.add(page);
                            hBaseAddTimerContext.stop();
                        }
                        if (isAddedToHBase) {
                            elasticSaveTimer.time(() -> elasticDAO.save(page));
                        } else {
                            logger.warn("Unable to add page with link {} to HBase", page.getLink());
                        }
                        crawledPages.inc();
                    }
                    redisAddTimer.time(() -> redisDAO.add(siteLink));
                    cache.put(siteDomain, LocalDateTime.now());
                }
            } else {
                links.add(siteLink);
            }
        } catch (URISyntaxException e) {
            logger.warn("Illegal URL format: " + siteLink, e);
        } catch (HBaseException e) {
            logger.error("Unable to establish HBase connection", e);
            links.clear();
            links.add(siteLink);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            LocalDateTime end = LocalDateTime.now();
            long duration = start.until(end, ChronoUnit.MILLIS);
            if (isLinkSkipped) {
                skippedTimeHistogram.update(duration);
                skippedCounter.inc(duration);
                TotalSkip.addAndGet(duration);
            } else {
                crawledHistogram.update(duration);
                crawledCounter.inc(duration);
            }
            TotalCrawl.addAndGet(duration);
            System.out.println("skip: " + TotalSkip.get());
            System.out.println("crawl: " + TotalCrawl.get());
            System.out.println("divide: " + (TotalSkip.get() * 1.0) / TotalCrawl.get());
            System.out.println("---------");
        }
        return links;
    }

    /**
     * crawl a site and return it's content as a page
     *
     * @param link link of site
     * @return page if able to crawl page
     */
    public Optional<Page> getPage(String link) {
        Timer.Context context = getPageTimer.time();
        try {
            Optional<Document> documentOptional = parserService.getDocument(link);
            if (!documentOptional.isPresent()) {
                return Optional.empty();
            }
            Document document = documentOptional.get();
            String pageContentWithoutTag = document.text().replace("\n", " ");
            if (pageContentWithoutTag.isEmpty()) {
                logger.warn("There is no content for site: {}", link);
            } else if (parserService.isEnglishLanguage(pageContentWithoutTag)) {
                Set<Anchor> anchors = parserService.getAnchors(document);
                List<Meta> metas = parserService.getMetas(document);
                String title = parserService.getTitle(document);
                Page page = new Page(link, title, pageContentWithoutTag, anchors, metas, 1.0);
                return Optional.of(page);
            }
        } catch (MalformedURLException e) {
            logger.warn("Unable to reverse link: {}", link);
        } catch (LanguageDetectException e) {
            logger.warn("Cannot detect language of site: {}", link);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            context.stop();
        }
        return Optional.empty();
    }
}
