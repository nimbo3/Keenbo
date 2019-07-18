package in.nimbo;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.config.AppConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.service.CrawlerService;
import in.nimbo.service.ParserService;
import in.nimbo.service.kafka.KafkaService;

import java.time.LocalDateTime;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) {
        ElasticDAO elasticDAO = null; // TODO must implemented
        HBaseDAO hBaseDAO = null; // TODO must implemented
        AppConfig appConfig = AppConfig.load();
        ParserService parserService = new ParserService(appConfig);
        Cache<String, LocalDateTime> cache = Caffeine.newBuilder().maximumSize(appConfig.getCaffeineMaxSize())
                .expireAfterWrite(appConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();
        CrawlerService crawlerService = new CrawlerService(appConfig, cache, hBaseDAO, elasticDAO, parserService);
        KafkaService kafkaService = new KafkaService(crawlerService);
        kafkaService.schedule();

        System.out.println("Welcome to Search Engine");
        System.out.print("engine> ");
        Scanner in = new Scanner(System.in);
        while (in.hasNextLine()) {
            String link = in.nextLine();
            kafkaService.sendMessage(link);
            System.out.print("engine> ");
        }
    }
}
