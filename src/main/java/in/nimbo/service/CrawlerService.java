package in.nimbo.service;

import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.config.AppConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.entity.Link;
import in.nimbo.entity.Page;
import in.nimbo.service.kafka.KafkaProducerConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
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
            Link urlLink = new Link(siteLink);
            if (cache.getIfPresent(urlLink.getDomain()) == null && !hBaseDAO.contains(siteLink)) {
                Page page = parserService.parse(siteLink);
                links.addAll(page.getLinks());
                // TODO implements interfaces
//                elasticDAO.save(siteLink, page.getContent());
//                hBaseDAO.add(siteLink);
                cache.put(urlLink.getDomain(), LocalDateTime.now());
            }
        } catch (MalformedURLException e) {
            logger.error("Illegal url format: " + siteLink, e);
        }
        return links;
    }
}
