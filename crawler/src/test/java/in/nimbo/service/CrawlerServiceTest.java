package in.nimbo.service;

import com.codahale.metrics.SharedMetricRegistries;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.TestUtility;
import in.nimbo.common.config.AppConfig;
import in.nimbo.common.exception.HBaseException;
import in.nimbo.common.exception.LanguageDetectException;
import in.nimbo.dao.elastic.ElasticDAO;
import in.nimbo.dao.hbase.HBaseDAO;
import in.nimbo.dao.redis.RedisDAO;
import in.nimbo.entity.Anchor;
import in.nimbo.entity.Meta;
import in.nimbo.entity.Page;
import in.nimbo.common.utility.LinkUtility;
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
    private static Set<Anchor> anchors;
    private static List<Meta> metas;
    private static final String FILE_ADDRESS = "src/test/resources/html/sampleEnglish.html";
    private String contentWithoutTag;

    @BeforeClass
    public static void init() {
        elasticDAO = mock(ElasticDAO.class);
        parserService = spy(new ParserService(new AppConfig()));
        appConfig = AppConfig.load();
        SharedMetricRegistries.setDefault("Keenbo");
    }

    @Before
    public void beforeEachTest() throws MalformedURLException {
        link = "http://nimbo.in/";
        invalidLink = "abc";
        contentWithoutTag = "nimbo Hi Header support@nimbo.in paragraph! another link";
        String title = "nimbo";
        anchors = new HashSet<>();
        anchors.add(new Anchor("https://google.com", "another link"));
        anchors.add(new Anchor("http://sahab.com", "Hi"));
        metas = new ArrayList<>();
        metas.add(new Meta("nimbo", "sahab"));
        metas.add(new Meta("google", "search"));
        crawledLinks = anchors.stream().map(Anchor::getHref).collect(Collectors.toSet());
        Page page = new Page(link, title, contentWithoutTag, anchors, new ArrayList<>(), 1);
        hBaseDAO = mock(HBaseDAO.class);
        redisDAO = mock(RedisDAO.class);
        String input = TestUtility.getFileContent(Paths.get(FILE_ADDRESS));
        document = Jsoup.parse(input, "UTF-8");
        when(parserService.getDocument(link)).thenReturn(Optional.of(document));
        doReturn(true).when(parserService).isEnglishLanguage(anyString());
        doNothing().when(elasticDAO).save(any(Page.class));
        doReturn(true).when(hBaseDAO).add(any(Page.class));
        cache = Caffeine.newBuilder().maximumSize(appConfig.getCaffeineMaxSize())
                .expireAfterWrite(appConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();
        crawlerService = spy(new CrawlerService(cache, hBaseDAO, elasticDAO, parserService));
    }

    @Test
    public void crawlTest() {
        when(hBaseDAO.contains(link)).thenReturn(false);
        Set<String> answer = crawlerService.crawl(link);
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
        Set<String> actualResult = new HashSet<>();
        actualResult.add(link);
        Set<String> answer = crawlerService.crawl(link);
        Assert.assertEquals(answer, actualResult);
    }

    @Test
    public void crawlRepeatedLinkTest() {
        when(hBaseDAO.contains(anyString())).thenReturn(true);
        Set<String> actualResult = new HashSet<>();
        Set<String> answer = crawlerService.crawl(link);
        Assert.assertEquals(actualResult, answer);
    }

    @Test
    public void crawlInvalidLink() {
        when(hBaseDAO.contains(link)).thenReturn(true);
        Set<String> answer = crawlerService.crawl("http://");
        Set<String> actualResult = new HashSet<>();
        Assert.assertEquals(answer, actualResult);
    }

    @Test
    public void crawlWithHBaseException() {
        when(hBaseDAO.add(any(Page.class))).thenThrow(HBaseException.class);
        Set<String> answer = crawlerService.crawl(link);
        HashSet<String> expected = new HashSet<>();
        expected.add(link);
        Assert.assertEquals(expected, answer);
    }

    @Test
    public void crawlWithHBaseAddFalse() {
        when(hBaseDAO.add(any(Page.class))).thenReturn(false);
        Set<String> answer = crawlerService.crawl(link);
        Assert.assertEquals(answer, crawledLinks);
    }

    @Test
    public void getPageTest() {
        Optional<Page> optionalPage = parserService.getPage(link);
        Assert.assertTrue(optionalPage.isPresent());
        Page returnPage = optionalPage.get();
        Assert.assertEquals(link, returnPage.getLink());
        Assert.assertEquals(contentWithoutTag, returnPage.getContent());
        String title = "nimbo";
        Assert.assertEquals(title, returnPage.getTitle());
        Assert.assertEquals(anchors, returnPage.getAnchors());
        Assert.assertEquals(metas, returnPage.getMetas());
    }

    @Test
    public void getPageWithEmptyDocumentTest() {
        when(parserService.getDocument(link)).thenReturn(Optional.empty());
        Optional<Page> optionalPage = parserService.getPage(link);
        Assert.assertFalse(optionalPage.isPresent());
    }

    @Test
    public void getPageMalformedURLExceptionTest() {
        when(parserService.getDocument(invalidLink)).thenReturn(Optional.of(document));
        Optional<Page> optionalPage = parserService.getPage(invalidLink);
        Assert.assertFalse(optionalPage.isPresent());
    }

    @Test
    public void getPageLanguageDetectExceptionTest() {
        doThrow(new LanguageDetectException(new Exception())).when(parserService).isEnglishLanguage(anyString());
        Optional<Page> optionalPage = parserService.getPage(invalidLink);
        Assert.assertFalse(optionalPage.isPresent());
    }
}
