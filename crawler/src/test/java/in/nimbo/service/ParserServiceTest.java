package in.nimbo.service;

import in.nimbo.TestUtility;
import in.nimbo.config.AppConfig;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.BeforeClass;

import java.nio.file.Paths;

import static org.mockito.Mockito.spy;

public class ParserServiceTest {
    private static Document document;
    private static Document persianDocument;
    private static ParserService parserService;
    private static final String FILE_ADDRESS = "src/test/resources/html/sampleEnglish.html";
    private static final String PERSIAN_FILE_ADDRESS = "src/test/resources/html/samplePersian.html";
    private static final String pageContent = "Nimbo Hi Header mail at support@nimbo.in. paragraph! another link";

    @BeforeClass
    public static void init() {
        AppConfig appConfig = AppConfig.load();
        String input = TestUtility.getFileContent(Paths.get(FILE_ADDRESS));
        String persianInput = TestUtility.getFileContent(Paths.get(PERSIAN_FILE_ADDRESS));
        document = Jsoup.parse(input, "UTF-8");
        persianDocument = Jsoup.parse(persianInput, "UTF-8");
        parserService = spy(new ParserService(appConfig));
    }

}
