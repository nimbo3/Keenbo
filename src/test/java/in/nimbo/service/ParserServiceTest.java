package in.nimbo.service;

import in.nimbo.TestUtility;
import in.nimbo.config.AppConfig;
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

import static org.mockito.Mockito.*;

public class ParserServiceTest {
    private static String input;
    private static String persianInput;
    private static Document document;
    private static Document persianDocument;
    private static ParserService parserService;
    private static final String FILE_ADDRESS = "src/test/resources/html/sampleHTML.html";
    private static final String PERSIAN_FILE_ADDRESS = "src/test/resources/html/sampleHTMLper.html";
    private static String link = "https://google.com";
    private static String pageContent = "Nimbo Link Header mail at support@nimbo.in. paragraph! another link";
    private static List<String> pageLinks = new ArrayList<>();

    @BeforeClass
    public static void init() {
        AppConfig appConfig = AppConfig.load();
        input = TestUtility.getFileContent(Paths.get(FILE_ADDRESS));
        persianInput = TestUtility.getFileContent(Paths.get(PERSIAN_FILE_ADDRESS));
        document = Jsoup.parse(input, "UTF-8");
        persianDocument = Jsoup.parse(persianInput, "UTF-8");
        parserService = spy(new ParserService(appConfig));
        pageLinks.add("http://nimbo.in");
        pageLinks.add("https://google.com");
    }

    @Test
    public void parseTest() {
        doReturn(Optional.of(document)).when(parserService).getDocument(link);
        Page page = parserService.parse(link).get();
        Assert.assertEquals(page.getContent(), pageContent);
        Assert.assertEquals(page.getLinks(), pageLinks);
    }

    @Test
    public void parseNotEnglishPageTest() {
        doReturn(Optional.of(persianDocument)).when(parserService).getDocument(link);
        Optional<Page> page = parserService.parse(link);
        Assert.assertFalse(page.isPresent());
    }
}
