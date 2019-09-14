package in.nimbo.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.TestUtility;
import in.nimbo.common.config.ProjectConfig;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Meta;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.InvalidLinkException;
import in.nimbo.common.exception.LanguageDetectException;
import in.nimbo.common.exception.ParseLinkException;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.dao.redis.RedisDAO;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class CrawlerServiceTest {
    private static RedisDAO redisDAO;
    private static ParserService parserService;
    private static Document documentWithoutTitle;
    private static ProjectConfig projectConfig;
    private static Cache<String, LocalDateTime> cache;
    private static CrawlerService crawlerService;
    private static String link;
    private static String invalidLink;
    private static Set<Anchor> anchors;
    private static List<Meta> metas;
    private static final String FILE_ADDRESS = "src/test/resources/html/sampleEnglish.html";
    private static final String FILE_WITHOUT_TITLE_ADDRESS = "src/test/resources/html/sampleEnglishWithoutTitle.html";
    private String contentWithoutTag;
    private Page page;

    @BeforeClass
    public static void init() {
        parserService = spy(new ParserService(new ProjectConfig()));
        projectConfig = ProjectConfig.load();
        TestUtility.setMetricRegistry();
    }

    @Before
    public void beforeEachTest() throws MalformedURLException {
        link = "http://nimbo.in/";
        invalidLink = "abc";
        contentWithoutTag = "nimbo Hi Header support@nimbo.in paragraph! another link";
        String title = "nimbo";
        anchors = new HashSet<>();
        anchors.add(new Anchor("https://google.com", "another link"));
        anchors.add(new Anchor("http://sahab.com", "hi"));
        metas = new ArrayList<>();
        metas.add(new Meta("nimbo", "sahab"));
        metas.add(new Meta("google", "search"));
        page = new Page(link, title, contentWithoutTag, anchors, metas, 1.0);
        redisDAO = mock(RedisDAO.class);
        String input = TestUtility.getFileContent(Paths.get(FILE_ADDRESS));
        String inputWithoutTitle = TestUtility.getFileContent(Paths.get(FILE_WITHOUT_TITLE_ADDRESS));
        Document document = Jsoup.parse(input, "UTF-8");
        documentWithoutTitle = Jsoup.parse(inputWithoutTitle, "UTF-8");
        when(parserService.getDocument(any())).thenReturn(document);
        doReturn(true).when(parserService).isEnglishLanguage(anyString());
        cache = Caffeine.newBuilder().maximumSize(projectConfig.getCaffeineMaxSize())
                .expireAfterWrite(projectConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();
        crawlerService = spy(new CrawlerService(cache, redisDAO, parserService));
    }

    @Test
    public void crawlTest() {
        when(redisDAO.contains(link)).thenReturn(false);
        Page returnedPage = crawlerService.crawl(link).get();
        Assert.assertEquals(page.getLink(), returnedPage.getLink());
        Assert.assertEquals(page.getAnchors(), returnedPage.getAnchors());
        Assert.assertEquals(page.getMetas(), returnedPage.getMetas());
        Assert.assertEquals(page.getContent(), returnedPage.getContent());
        Assert.assertEquals(page.getTitle(), returnedPage.getTitle());
        Assert.assertEquals(page.getLinkDepth(), returnedPage.getLinkDepth());
        Assert.assertEquals(page.getReversedLink(), returnedPage.getReversedLink());
    }

    @Test
    public void crawlCachedLinkTest() {
        when(redisDAO.contains(link)).thenReturn(false);
        try {
            cache.put(LinkUtility.getMainDomain(link), LocalDateTime.now());
        } catch (MalformedURLException e) {
            Assert.fail();
        }
        Optional<Page> returnedPage = crawlerService.crawl(link);
        Assert.assertFalse(returnedPage.isPresent());
    }

    @Test(expected = InvalidLinkException.class)
    public void crawlRepeatedLinkTest() {
        when(redisDAO.contains(anyString())).thenReturn(true);
        Optional<Page> returnedPage = crawlerService.crawl(link);
        Assert.fail();
    }

    @Test(expected = InvalidLinkException.class)
    public void crawlInvalidLink() {
        Optional<Page> returnedPage = crawlerService.crawl("http://");
        Assert.fail();
    }

    @Test
    public void getPageTest() {
        Page returnedPage = crawlerService.getPage(link);
        Assert.assertEquals(link, returnedPage.getLink());
        Assert.assertEquals(contentWithoutTag, returnedPage.getContent());
        String title = "nimbo";
        Assert.assertEquals(title, returnedPage.getTitle());
        Assert.assertEquals(anchors, returnedPage.getAnchors());
        Assert.assertEquals(metas, returnedPage.getMetas());
    }

    @Test
    public void getPageWithoutTitleTest() {
        when(parserService.getDocument(any())).thenReturn(documentWithoutTitle);
        Page returnedPage = crawlerService.getPage(link);
        Assert.assertEquals(link, returnedPage.getLink());
        String contentWithoutTag = "Hi Header support@nimbo.in paragraph! another link";
        Assert.assertEquals(contentWithoutTag, returnedPage.getContent());
        Assert.assertEquals(link, returnedPage.getTitle());
        Assert.assertEquals(anchors, returnedPage.getAnchors());
        Assert.assertEquals(metas, returnedPage.getMetas());
    }

    @Test(expected = ParseLinkException.class)
    public void getPageMalformedURLExceptionTest() {
        when(parserService.getDocument(any())).thenThrow(MalformedURLException.class);
        Page returnedPage = crawlerService.getPage(invalidLink);
        Assert.fail();
    }

    @Test(expected = ParseLinkException.class)
    public void getPageLanguageDetectExceptionTest() {
        doThrow(LanguageDetectException.class).when(parserService).isEnglishLanguage(anyString());
        Page returnedPage = crawlerService.getPage(link);
        Assert.fail();
    }
}
