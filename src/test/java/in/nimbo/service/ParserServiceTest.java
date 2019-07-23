package in.nimbo.service;

import in.nimbo.TestUtility;
import in.nimbo.config.AppConfig;
import in.nimbo.entity.Link;
import in.nimbo.entity.Page;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.*;

public class ParserServiceTest {
    private static Document document;
    private static Document persianDocument;
    private static ParserService parserService;
    private static final String FILE_ADDRESS = "src/test/resources/html/sampleHTML.html";
    private static final String PERSIAN_FILE_ADDRESS = "src/test/resources/html/sampleHTMLper.html";
    private String link;
    private List<String> pageLinks;

    @BeforeClass
    public static void init() {
        AppConfig appConfig = AppConfig.load();
        String input = TestUtility.getFileContent(Paths.get(FILE_ADDRESS));
        String persianInput = TestUtility.getFileContent(Paths.get(PERSIAN_FILE_ADDRESS));
        document = Jsoup.parse(input, "UTF-8");
        persianDocument = Jsoup.parse(persianInput, "UTF-8");
        parserService = spy(new ParserService(appConfig));
    }

    @Before
    public void beforeEachtest() {
        link = "https://google.com";
        pageLinks = new ArrayList<>();
        pageLinks.add("http://nimbo.in");
        pageLinks.add(link);
    }

    @Test
    public void parseTest() {
        doReturn(Optional.of(document)).when(parserService).getDocument(link);
        doReturn(true).when(parserService).isEnglishLanguage(anyString());
        assertTrue(parserService.parse(link).isPresent());
        Page page = parserService.parse(link).get();
        String pageContent = "Nimbo Link Header mail at support@nimbo.in. paragraph! another link";
        Assert.assertEquals(page.getContentWithOutTags(), pageContent);
        List<String> list = new ArrayList<>();
        for (Link link : page.getLinks()) {
            list.add(link.getHref());
        }
        Assert.assertEquals(list, pageLinks);
    }

    @Test
    public void parseNotEnglishPageTest() {
        doReturn(Optional.of(persianDocument)).when(parserService).getDocument(link);
        doReturn(false).when(parserService).isEnglishLanguage(anyString());
        Optional<Page> page = parserService.parse(link);
        Assert.assertFalse(page.isPresent());
    }

    @Test
    public void emptyDocumentTest() {
        doReturn(Optional.empty()).when(parserService).getDocument(link);
        doReturn(true).when(parserService).isEnglishLanguage(anyString());
        Optional<Page> page = parserService.parse(link);
        Assert.assertFalse(page.isPresent());
    }
}
