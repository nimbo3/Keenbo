package in.nimbo;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.conf.Config;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.service.ParserService;
import in.nimbo.service.kafka.KafkaService;
import in.nimbo.service.CrawlerServiceImpl;
import in.nimbo.service.schedule.ScheduleCrawling;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) throws IOException {
        Config config = loadConfig();
        ElasticDAO elasticDAO = null;
        HBaseDAO hBaseDAO = null;
        KafkaService kafkaService = null;
        ParserService parserService = new ParserService();
        Cache<Object, Object> cache = Caffeine.newBuilder().maximumSize(config.getMaximumSize())
                .expireAfterWrite(config.getExpireCacheTime(), TimeUnit.SECONDS).build();
        CrawlerServiceImpl crawlerServiceImpl = new CrawlerServiceImpl(cache, kafkaService, hBaseDAO, elasticDAO, parserService, config);
        ScheduleCrawling scheduleCrawling = new ScheduleCrawling(crawlerServiceImpl, config);
        System.out.println("Hello World!");
    }

    private static Config loadConfig() throws IOException {
        Properties properties = new Properties();
        InputStream stream = Thread.currentThread().
                getContextClassLoader().getResourceAsStream("conf.properties");
        properties.load(stream);
        int timeoutMillisecond = Integer.valueOf(properties.getProperty("in.nimbo.conf.Conf.timeout.millisecond"));
        int maximumSize = Integer.valueOf(properties.getProperty("in.nimbo.conf.Conf.size.maximum"));
        int expireCacheTime = Integer.valueOf(properties.getProperty("in.nimbo.conf.Conf.timeout.cache.second"));
        String topic = properties.getProperty("in.nimbo.conf.Conf.kafka.topic");
        return new Config(timeoutMillisecond, maximumSize, expireCacheTime, topic);
    }
}
