package in.nimbo.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.config.AppConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.entity.Page;
import in.nimbo.exception.HBaseException;
import in.nimbo.utility.LinkUtility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class CrawlerServiceTest {
    private static HBaseDAO hBaseDAO;
    private static ElasticDAO elasticDAO;
    private static ParserService parserService;
    private static AppConfig appConfig;
    private static Cache<String, LocalDateTime> cache;
    private static CrawlerService crawlerService;
    private static Optional<Page> page;
    private static String link;
    private static List<String> crawledLinks;

    @BeforeClass
    public static void init() {
        elasticDAO = mock(ElasticDAO.class);
        parserService = mock(ParserService.class);
        appConfig = AppConfig.load();
    }

    @Before
    public void beforeEachTest() {
        link = "http://nimbo.in/";
        String content = "Be your best!";
        crawledLinks = new ArrayList<>();
        crawledLinks.add("https://www.google.com/");
        crawledLinks.add("https://stackoverflow.com/");
        crawledLinks.add("https://www.sahab.ir/");
        page = Optional.of(new Page(content, crawledLinks));
        hBaseDAO = mock(HBaseDAO.class);
        when(parserService.parse(link)).thenReturn(page);
        doNothing().when(elasticDAO).save(link, content);
        doNothing().when(hBaseDAO).add(link);
        cache = Caffeine.newBuilder().maximumSize(appConfig.getCaffeineMaxSize())
                .expireAfterWrite(appConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();
//        crawlerService = new CrawlerService(appConfig, cache, hBaseDAO, elasticDAO, parserService);
    }

    @Test
    public void crawlTest() {
        when(hBaseDAO.contains(link)).thenReturn(false);
        List<String> answer = crawlerService.crawl(link);
        Assert.assertEquals(answer, crawledLinks);
    }

    @Test
    public void crawlCachedLinkTest() {
        when(hBaseDAO.contains(link)).thenReturn(false);
        try {
            cache.put(LinkUtility.getMainDomain(link), LocalDateTime.now());
        } catch (URISyntaxException e) {
            Assert.fail();
        }
        List<String> actualResult = new ArrayList<>();
        actualResult.add(link);
        List<String> answer = crawlerService.crawl(link);
        Assert.assertEquals(answer, actualResult);
    }

    @Test
    public void crawlRepeatedLinkTest() {
        when(hBaseDAO.contains(link)).thenReturn(true);
        List<String> actualResult = new ArrayList<>();
        List<String> answer = crawlerService.crawl(link);
        Assert.assertEquals(answer, actualResult);
    }

    @Test
    public void crawlInvalidLink() {
        when(hBaseDAO.contains(link)).thenReturn(true);
        List<String> answer = crawlerService.crawl("http://");
        List<String> actualResult = new ArrayList<>();
        Assert.assertEquals(answer, actualResult);
    }

    @Test
    public void throwHBaseExeptionTest() {
        when(hBaseDAO.contains(link)).thenThrow(new HBaseException());
        List<String> actualResult = new ArrayList<>();
        List<String> answer = crawlerService.crawl(link);
        Assert.assertEquals(answer, actualResult);
    }
}