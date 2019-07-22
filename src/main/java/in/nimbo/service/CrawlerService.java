package in.nimbo.service;

import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.config.AppConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.entity.Page;
import in.nimbo.exception.HBaseException;
import in.nimbo.service.kafka.Consumer;
import in.nimbo.utility.LinkUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CrawlerService {
    private Logger logger = LoggerFactory.getLogger(Consumer.class);
    private Cache<String, LocalDateTime> cache;
    private HBaseDAO hBaseDAO;
    private ElasticDAO elasticDAO;
    private ParserService parserService;
    private AppConfig appConfig;

    public CrawlerService(AppConfig appConfig,
                          Cache<String, LocalDateTime> cache,
                          HBaseDAO hBaseDAO, ElasticDAO elasticDAO,
                          ParserService parserService) {
        this.appConfig = appConfig;
        this.cache = cache;
        this.hBaseDAO = hBaseDAO;
        this.elasticDAO = elasticDAO;
        this.parserService = parserService;
    }

    public List<String> crawl(String siteLink) {
        List<String> links = new ArrayList<>();
        try {
            String siteDomain = LinkUtility.getMainDomain(siteLink);
            if (cache.getIfPresent(siteDomain) == null) {
                if (!hBaseDAO.contains(siteLink)) {
                    Optional<Page> page = parserService.parse(siteLink);
                    page.ifPresent(value -> links.addAll(value.getLinks()));
                    // TODO implements interfaces
//                  elasticDAO.save(siteLink, page.getContent());
                    hBaseDAO.add(siteLink);
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
