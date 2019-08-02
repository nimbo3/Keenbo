package in.nimbo.service;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.common.exception.HBaseException;
import in.nimbo.common.exception.LanguageDetectException;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.entity.Anchor;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
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

public class CrawlerService {
    private final Counter crawledPages;
    private final Timer getPageTimer;
    private final Timer hBaseContainTimer;
    private final Timer elasticSaveTimer;
    private final Timer hBaseAddTimer;
    private final Counter skippedCounter;
    private final Counter crawledCounter;

    private Logger appLogger = LoggerFactory.getLogger("app");
    private Logger parserLogger = LoggerFactory.getLogger("parser");
    private Cache<String, LocalDateTime> cache;
    private HBaseDAO hBaseDAO;
    private ElasticDAO elasticDAO;
    private ParserService parserService;

    public CrawlerService(Cache<String, LocalDateTime> cache,
                          HBaseDAO hBaseDAO, ElasticDAO elasticDAO,
                          ParserService parserService) {
        this.cache = cache;
        this.hBaseDAO = hBaseDAO;
        this.elasticDAO = elasticDAO;
        this.parserService = parserService;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        crawledPages = metricRegistry.counter(MetricRegistry.name(CrawlerService.class, "crawledPages"));
        getPageTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "getPage"));
        hBaseContainTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "hBaseContains"));
        elasticSaveTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "elasticSave"));
        hBaseAddTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "hBaseAdd"));
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

                Timer.Context hBaseContainContext = hBaseContainTimer.time();
                boolean contains = hBaseDAO.contains(siteLink);
                hBaseContainContext.stop();

                if (!contains) {
                    isLinkSkipped = false;
                    Timer.Context context = getPageTimer.time();
                    Optional<Page> pageOptional = parserService.getPage(siteLink);
                    context.stop();
                    cache.put(siteDomain, LocalDateTime.now());
                    if (pageOptional.isPresent()) {
                        Page page = pageOptional.get();
                        page.getAnchors().forEach(anchor -> links.add(anchor.getHref()));
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
                            appLogger.warn("Unable to add page with link {} to HBase", page.getLink());
                        }
                        crawledPages.inc();
                    }
                    cache.put(siteDomain, LocalDateTime.now());
                }
            } else {
                links.add(siteLink);
            }
        } catch (URISyntaxException e) {
            parserLogger.warn("Illegal URL format: " + siteLink, e);
        } catch (HBaseException e) {
            appLogger.error("Unable to establish HBase connection", e);
            links.clear();
            links.add(siteLink);
            appLogger.info("Retry link {} again because of HBase exception", siteLink);
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
        return links;
    }
}
