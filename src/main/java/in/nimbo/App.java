package in.nimbo;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.config.AppConfig;
import in.nimbo.config.HBaseConfig;
import in.nimbo.config.KafkaConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.hbase.HBaseDAOImpl;
import in.nimbo.service.CrawlerService;
import in.nimbo.service.ParserService;
import in.nimbo.service.kafka.KafkaService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.time.LocalDateTime;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class App {

    public static void main(String[] args) {
        ElasticDAO elasticDAO = null; // TODO must implemented
        Configuration configuration = HBaseConfiguration.create();
        HBaseConfig config = HBaseConfig.load();
        AppConfig appConfig = AppConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();

        HBaseDAO hBaseDAO = new HBaseDAOImpl(configuration, config);
        ParserService parserService = new ParserService(appConfig);
        Cache<String, LocalDateTime> cache = Caffeine.newBuilder().maximumSize(appConfig.getCaffeineMaxSize())
                .expireAfterWrite(appConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();
        CrawlerService crawlerService = new CrawlerService(appConfig, cache, hBaseDAO, elasticDAO, parserService);
        KafkaService kafkaService = new KafkaService(crawlerService, kafkaConfig);
        kafkaService.schedule();

        System.out.println("Welcome to Search Engine");
        System.out.print("engine> ");
        Scanner in = new Scanner(System.in);
        while (in.hasNext()) {
            String cmd = in.next();
            if (cmd.equals("add")) {
                String link = in.next();
                kafkaService.sendMessage(link);
            } else if (cmd.equals("exit")) {
                kafkaService.stopSchedule();
                break;
            }
            System.out.print("engine> ");
        }
    }
}
