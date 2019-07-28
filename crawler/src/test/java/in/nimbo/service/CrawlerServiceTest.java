package in.nimbo.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.TestUtility;
import in.nimbo.config.AppConfig;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.redis.RedisDAO;
import in.nimbo.entity.Anchor;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import in.nimbo.utility.LinkUtility;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

public class CrawlerServiceTest {
    private static HBaseDAO hBaseDAO;
    private static RedisDAO redisDAO;
    private static ElasticDAO elasticDAO;
    private static ParserService parserService;
    private static Document document;
    private static AppConfig appConfig;
    private static Cache<String, LocalDateTime> cache;
    private static CrawlerService crawlerService;
    private static String link;
    private static String invalidLink;
    private static Set<String> crawledLinks;
    private static Page page;
    private static String input;
    private static final String FILE_ADDRESS = "src/test/resources/html/sampleEnglish.html";

    @BeforeClass
    public static void init() {
        elasticDAO = mock(ElasticDAO.class);
        parserService = mock(ParserService.class);
        appConfig = AppConfig.load();
    }

    @Before
    public void beforeEachTest() throws MalformedURLException {
        link = "http://nimbo.in/";
        invalidLink = "abc";
        String contentWithTag = "Be your best!";
        String contentWithoutTag = "<html>Be your best!</html>";
        String title = "nimbo";
        Set<Anchor> anchors = new HashSet<>();
        anchors.add(new Anchor("https://www.google.com/", "google"));
        anchors.add(new Anchor("https://stackoverflow.com/", "stackoverflow"));
        anchors.add(new Anchor("https://www.sahab.ir/", "sahab"));
        crawledLinks = anchors.stream().map(Anchor::getHref).collect(Collectors.toSet());
        List<Meta> metas = new ArrayList<>();
        metas.add(new Meta("key1", "value1"));
        metas.add(new Meta("key2", "value2"));
        page = new Page(link, title, contentWithTag, contentWithoutTag, anchors, metas, 1);
        hBaseDAO = mock(HBaseDAO.class);
        redisDAO = mock(RedisDAO.class);
        input = TestUtility.getFileContent(Paths.get(FILE_ADDRESS));
        document = Jsoup.parse(input, "UTF-8");
        //document = mock(Document.class);
        when(parserService.getDocument(link)).thenReturn(Optional.of(document));
        when(parserService.getAnchors(document)).thenReturn(anchors);
        when(parserService.getMetas(document)).thenReturn(metas);
        when(parserService.getTitle(document)).thenReturn(title);
        doReturn(true).when(parserService).isEnglishLanguage(anyString());
        doNothing().when(elasticDAO).save(any(Page.class));
        doReturn(true).when(hBaseDAO).add(any(Page.class));
        cache = Caffeine.newBuilder().maximumSize(appConfig.getCaffeineMaxSize())
                .expireAfterWrite(appConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();
        crawlerService = spy(new CrawlerService(cache, hBaseDAO, elasticDAO, parserService, redisDAO));
    }

    @Test
    public void crawlTest() {
        doReturn(Optional.of(page)).when(crawlerService).getPage(anyString());
        when(redisDAO.contains(link)).thenReturn(false);
        Set<String> answer = crawlerService.crawl(link);
        Assert.assertEquals(answer, crawledLinks);
    }

    @Test
    public void crawlCachedLinkTest() {
        when(redisDAO.contains(link)).thenReturn(false);
        try {
            cache.put(LinkUtility.getMainDomain(link), LocalDateTime.now());
        } catch (URISyntaxException e) {
            Assert.fail();
        }
        Set<String> actualResult = new HashSet<>();
        actualResult.add(link);
        Set<String> answer = crawlerService.crawl(link);
        Assert.assertEquals(answer, actualResult);
    }

    @Test
    public void crawlRepeatedLinkTest() {
        when(redisDAO.contains(link)).thenReturn(true);
        Set<String> actualResult = new HashSet<>();
        Set<String> answer = crawlerService.crawl(link);
        Assert.assertEquals(answer, actualResult);
    }

    @Test
    public void crawlInvalidLink() {
        when(redisDAO.contains(link)).thenReturn(true);
        Set<String> answer = crawlerService.crawl("http://");
        Set<String> actualResult = new HashSet<>();
        Assert.assertEquals(answer, actualResult);
    }

    @Test
    public void getPageWithEmptyDocumentTest() {
        when(parserService.getDocument(link)).thenReturn(Optional.empty());
        Optional<Page> optionalPage = crawlerService.getPage(link);
        Assert.assertFalse(optionalPage.isPresent());
    }

    @Test
    public void getPageMalformedURLExceptionTest() {
        when(parserService.getDocument(invalidLink)).thenReturn(Optional.of(document));
        Optional<Page> optionalPage = crawlerService.getPage(invalidLink);
        Assert.assertFalse(optionalPage.isPresent());
    }
}