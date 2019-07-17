package in.nimbo.service;

import com.github.benmanes.caffeine.cache.Cache;
import in.nimbo.conf.Config;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.entity.Link;
import in.nimbo.entity.Page;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.List;

public class CrawlerServiceImpl implements CrawlerService {
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

    public void crawl() throws MalformedURLException {
        while (true) {
//            String poll = kafkaService.receive(config.getLinksTopic());
            String poll = null;
            URL url = new URL(poll);
            Link process = process(url);
            if (cache.getIfPresent(process.getDomain()) == null) {
                if (!hBaseDAO.contains(poll)) {
                    try {
                        Page page = parserService.parse(poll);
                        for (String link : page.getLinks()) {
                            String completeLink = getCompleteLink(poll, url, link);
//                            kafkaService.send(config.getLinksTopic(), completeLink);
                        }
                        elasticDAO.save(poll, page.getContent());
                        hBaseDAO.add(poll);
                        cache.put(process.getDomain(), new Date());
                    } catch (Exception e) {
                        System.out.println("error: " + poll);
                    }
                }
            } else {
//                kafkaService.send(config.getLinksTopic(), poll);
            }
        }
    }

    private Link process(URL url) {
        String protocol = url.getProtocol();
        int port = url.getPort();
        String path = url.getPath();
        String host = url.getHost();
        return new Link(protocol, host, port, path);
    }

    private String getCompleteLink(String base, URL url, String href) {
        if (href.startsWith("//")) {
            Link process = process(url);
            return process.getProtocol() + ":" + href;
        }
        if (href.startsWith("/")) {
            Link process = process(url);
            return process.getProtocol() + "://" + process.getHost() + href;
        }
        if (href.startsWith("#")) {
            return base;
        }
        try {
            URL u = new URL(href);
            Link process = process(u);
            return process.getProtocol() + "://" + process.getHost() + process.getUri();
        } catch (MalformedURLException e) {
            Link process = process(url);
            if (process.getUri() == null || process.getUri().isEmpty()) {
                return base + "/" + href;
            }
            return base.substring(0, base.lastIndexOf("/")) + "/" + href;
        }
    }

    @Override
    public List<String> crawl(String link) {
        return null;
    }
}
