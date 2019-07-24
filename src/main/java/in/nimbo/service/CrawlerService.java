package in.nimbo.service;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.redis.RedisDAO;
import in.nimbo.entity.Anchor;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import in.nimbo.exception.HBaseException;
import in.nimbo.exception.LanguageDetectException;
import in.nimbo.service.kafka.Consumer;
import in.nimbo.utility.LinkUtility;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CrawlerService {
    private static final int ENGLISH_PROBABILITY = 80;
    private Logger logger = LoggerFactory.getLogger(Consumer.class);
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
    }

    public List<String> crawl(String siteLink) {
        List<String> links = new ArrayList<>();
        try {
            String siteDomain = LinkUtility.getMainDomain(siteLink);
            if (cache.getIfPresent(siteDomain) == null) {
                if (!redisDAO.contains(siteLink)) {
                    Optional<Page> pageOptional = getPage(siteLink);
                    if (pageOptional.isPresent()) {
                        Page page = pageOptional.get();
                        page.getAnchors().forEach(link -> links.add(link.getHref()));
                        elasticDAO.save(page);
                        hBaseDAO.add(page);
                    }
                    redisDAO.add(siteLink);
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
        }
        return links;
    }

    /**
     * crawl a site and return it's content as a page
     * @param link link of site
     * @return page if able to crawl page
     */
    public Optional<Page> getPage(String link) {
        try {
            Optional<Document> documentOptional = parserService.getDocument(link);
            if (!documentOptional.isPresent()) {
                return Optional.empty();
            }
            Document document = documentOptional.get();
            String pageContentWithoutTag = document.text().replace("\n", " ");
            String pageContentWithTag = document.html();
            if (isEnglishLanguage(pageContentWithoutTag)) {
                List<Anchor> anchors = parserService.getAnchors(document);
                List<Meta> metas = parserService.getMetas(document);
                String title = parserService.getTitle(document);
                return Optional.of(new Page(link, title, pageContentWithTag, pageContentWithoutTag, anchors, metas, 1.0));
            }
        } catch (LanguageDetectException e) {
            logger.warn("Cannot detect language of site: {}", link);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return Optional.empty();
    }

    /**
     * @param text text
     * @return true if text is in English
     */
    public boolean isEnglishLanguage(String text) {
        try {
            Detector detector = DetectorFactory.create();
            detector.append(text);
            detector.setAlpha(0);
            return detector.getProbabilities().stream()
                    .anyMatch(x -> x.lang.equals("en") && x.prob > ENGLISH_PROBABILITY);
        } catch (LangDetectException e) {
            throw new LanguageDetectException(e);
        }
    }
}
