package in.nimbo;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.config.AppConfig;
import in.nimbo.config.ParserConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.service.CrawlerService;
import in.nimbo.service.ParserService;
import in.nimbo.service.kafka.KafkaService;
import in.nimbo.service.CrawlerServiceImpl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) throws IOException {
        AppConfig appConfig = loadConfig();
        ElasticDAO elasticDAO = null;
        HBaseDAO hBaseDAO = null;
        ParserConfig parserConfig = new ParserConfig(PARSER_TIMEOUT);
        ParserService parserService = new ParserService(parserConfig);
        Cache<Object, Object> cache = Caffeine.newBuilder().maximumSize(appConfig.getMaximumSize())
                .expireAfterWrite(appConfig.getExpireCacheTime(), TimeUnit.SECONDS).build();
        CrawlerServiceImpl crawlerServiceImpl = new CrawlerServiceImpl(cache, hBaseDAO, elasticDAO, parserService, appConfig);
        CrawlerService crawl = new CrawlerService() {
            @Override
            public List<String> crawl(String link) {
                return Collections.singletonList(link);
            }
            @Override
            public boolean isCached(String link) {
                return false;
            }
        };
        KafkaService kafkaService = new KafkaService(crawl);
        kafkaService.schedule();
        Scanner in = new Scanner(System.in);
        while (in.hasNextLine()) {
            String input = in.nextLine();
            kafkaService.sendMessage(input);
        }
    }

    private static AppConfig loadConfig() throws IOException {
        Properties properties = new Properties();
        InputStream stream = Thread.currentThread().
                getContextClassLoader().getResourceAsStream("app-config.properties");
        properties.load(stream);
        int timeoutMillisecond = Integer.valueOf(properties.getProperty("in.nimbo.conf.Conf.timeout.millisecond"));
        int maximumSize = Integer.valueOf(properties.getProperty("in.nimbo.conf.Conf.size.maximum"));
        int expireCacheTime = Integer.valueOf(properties.getProperty("in.nimbo.config.Conf.timeout.cache.second"));
        String topic = properties.getProperty("in.nimbo.conf.Conf.kafka.topic");
        return new AppConfig(timeoutMillisecond, maximumSize, expireCacheTime, topic);
    }
}
