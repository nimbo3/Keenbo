package in.nimbo.service;

import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.redis.RedisDAO;
import in.nimbo.entity.Page;
import in.nimbo.exception.HBaseException;
import in.nimbo.service.kafka.ConsumerService;
import in.nimbo.utility.LinkUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CrawlerService {
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
    }

    public List<String> crawl(String siteLink) {
        List<String> links = new ArrayList<>();
        try {
            String siteDomain = LinkUtility.getMainDomain(siteLink);
            if (cache.getIfPresent(siteDomain) == null) {
                if (!redisDAO.contains(siteLink)) {
                    Optional<Page> page = parserService.parse(siteLink);
                    page.ifPresent(value -> value.getAnchors().forEach(link -> links.add(link.getHref())));
                    page.ifPresent(value -> elasticDAO.save(value));
                    page.ifPresent(value -> hBaseDAO.add(value));
                    redisDAO.add(siteLink);
                    cache.put(siteDomain, LocalDateTime.now());
                    logger.info("get " + siteLink);
                }
            } else {
                links.add(siteLink);
            }
        } catch (URISyntaxException e) {
            logger.warn("Illegal url format: " + siteLink, e);
        } catch (HBaseException e) {
            logger.error("Unable to establish HBase connection", e);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return links;
    }
}
