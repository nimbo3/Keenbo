package in.nimbo.service;

import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.config.AppConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.utility.LinkUtility;
import in.nimbo.entity.Page;
import in.nimbo.service.kafka.KafkaProducerConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class CrawlerService {
    private Logger logger = LoggerFactory.getLogger(KafkaProducerConsumer.class);
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
            String siteDomain = LinkUtility.getDomain(siteLink);
            // TODO implements interfaces
//            if (cache.getIfPresent(urlLinkUtility.getDomain()) == null && !hBaseDAO.contains(siteLink)) {
            if (cache.getIfPresent(siteDomain) == null) {
                logger.info("get " + siteLink);
                Page page = parserService.parse(siteLink);
                links.addAll(page.getLinks());
                // TODO implements interfaces
//                elasticDAO.save(siteLink, page.getContent());
//                hBaseDAO.add(siteLink);
                cache.put(siteDomain, LocalDateTime.now());
            } else {
                logger.info("ignore " + siteLink);
            }
        } catch (URISyntaxException e) {
            logger.error("Illegal url format: " + siteLink, e);
        }
        return links;
    }
}
