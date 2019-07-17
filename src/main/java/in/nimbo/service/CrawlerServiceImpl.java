package in.nimbo.service;

import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.conf.Config;
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

public class CrawlerServiceImpl implements CrawlerService {
    private Logger logger = LoggerFactory.getLogger(KafkaProducerConsumer.class);
    private Cache<Object, Object> cache;
    private HBaseDAO hBaseDAO;
    private ElasticDAO elasticDAO;
    private ParserService parserService;
    private Config config;

    public CrawlerServiceImpl(Cache<Object, Object> cache, HBaseDAO hBaseDAO, ElasticDAO elasticDAO, ParserService parserService, Config config) {
        this.cache = cache;
        this.hBaseDAO = hBaseDAO;
        this.elasticDAO = elasticDAO;
        this.parserService = parserService;
        this.config = config;
    }

    @Override
    public List<String> crawl(String site_link) {
        List<String> links = new ArrayList<>();
        try {
            Link urlLink = new Link(site_link);
            if (!hBaseDAO.contains(site_link)) {
                Page page = parserService.parse(site_link);
                links.addAll(page.getLinks());
                elasticDAO.save(site_link, page.getContent());
                hBaseDAO.add(site_link);
                cache.put(urlLink.getDomain(), LocalDateTime.now());
            }
        } catch (MalformedURLException e) {
            logger.error("Illegal url format: " + site_link, e);
        }
        return links;
    }

    @Override
    public boolean isCached(String link) {
        try {
            Link url = new Link(link);
            return cache.getIfPresent(url.getDomain()) == null;
        } catch (MalformedURLException e) {
            logger.error("Illegal url format: " + link, e);
        }
        return false;
    }
}
