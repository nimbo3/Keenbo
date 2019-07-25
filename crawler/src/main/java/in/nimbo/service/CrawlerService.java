package in.nimbo.service;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
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

import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class CrawlerService {
    private final Timer getPageTimer;
    private final Timer redisContainsTimer;
    private final Timer elasticSaveTimer;
    private final Timer hBaseAddTimer;
    private final Timer redisAddTimer;
    private final Timer crawlTimer;

    private Logger logger = LoggerFactory.getLogger(ConsumerService.class);
    private Cache<String, LocalDateTime> cache;
    private HBaseDAO hBaseDAO;
    private ElasticDAO elasticDAO;
    private ParserService parserService;
    private RedisDAO redisDAO;

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
        getPageTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "getPage"));
        redisContainsTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "redisContains"));
        elasticSaveTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "elasticSave"));
        hBaseAddTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "hBaseAdd"));
        redisAddTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "redisAdd"));
        crawlTimer = metricRegistry.timer(MetricRegistry.name(CrawlerService.class, "crawl"));
    }

    public Set<String> crawl(String siteLink) {
        Timer.Context crawlTimerContext = crawlTimer.time();
        Set<String> links = new HashSet<>();
        try {
            String siteDomain = LinkUtility.getMainDomain(siteLink);
            if (cache.getIfPresent(siteDomain) == null) {

                Timer.Context redisContainsTimerContext = redisContainsTimer.time();
                boolean contains = !redisDAO.contains(siteLink);
                redisContainsTimerContext.stop();

                if (contains) {
                    Optional<Page> pageOptional = getPage(siteLink);
                    if (pageOptional.isPresent()) {
                        Page page = pageOptional.get();
                        page.getAnchors().forEach(link -> links.add(link.getHref()));

                        Timer.Context elasticSaveTimerContext = elasticSaveTimer.time();
                        elasticDAO.save(page);
                        elasticSaveTimerContext.stop();

                        Timer.Context hBaseAddTimerContext = hBaseAddTimer.time();
                        hBaseDAO.add(page);
                        hBaseAddTimerContext.stop();
                    }
                    Timer.Context redisAddTimerContext = redisAddTimer.time();
                    redisDAO.add(siteLink);
                    redisAddTimerContext.stop();

                    cache.put(siteDomain, LocalDateTime.now());
                    logger.info("get " + siteLink);
                }
            } else {
                links.add(siteLink);
            }
        } catch (URISyntaxException e) {
            logger.warn("Illegal URL format: " + siteLink, e);
        } catch (HBaseException e) {
            logger.error("Unable to establish HBase connection", e);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            crawlTimerContext.stop();
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
            String pageContentWithTag = document.html();
            if (parserService.isEnglishLanguage(pageContentWithoutTag)) {
                Set<Anchor> anchors = parserService.getAnchors(document);
                List<Meta> metas = parserService.getMetas(document);
                String title = parserService.getTitle(document);
                return Optional.of(new Page(link, title, pageContentWithTag, pageContentWithoutTag, anchors, metas, 1.0));
            }
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
