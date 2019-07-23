package in.nimbo;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.config.*;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.elastic.ElasticDAOImpl;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.hbase.HBaseDAOImpl;
import in.nimbo.dao.redis.RedisDAO;
import in.nimbo.dao.redis.RedisDAOImpl;
import in.nimbo.service.CrawlerService;
import in.nimbo.service.ParserService;
import in.nimbo.service.kafka.KafkaService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import redis.clients.jedis.JedisCluster;

import java.time.LocalDateTime;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class App {

    public static void main(String[] args) {
        try {
            DetectorFactory.loadProfile("profiles");
        } catch (LangDetectException e) {
            System.out.println("Unable to load profiles of language detector. Provide \"profile\" folder for language detector.");
        }
        Configuration configuration = HBaseConfiguration.create();
        HBaseConfig hBaseConfig = HBaseConfig.load();
        AppConfig appConfig = AppConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();
        ElasticConfig elasticConfig = ElasticConfig.load();
        RedisConfig redisConfig = RedisConfig.load();
        JedisCluster cluster = new JedisCluster(redisConfig.getHostAndPorts());

        ElasticDAO elasticDAO = new ElasticDAOImpl(elasticConfig);
        HBaseDAO hBaseDAO = new HBaseDAOImpl(configuration, hBaseConfig);
        RedisDAO redisDAO = new RedisDAOImpl(cluster, redisConfig);
        ParserService parserService = new ParserService(appConfig);
        Cache<String, LocalDateTime> cache = Caffeine.newBuilder().maximumSize(appConfig.getCaffeineMaxSize())
                .expireAfterWrite(appConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();
        CrawlerService crawlerService = new CrawlerService(cache, hBaseDAO, elasticDAO, parserService, redisDAO);
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
