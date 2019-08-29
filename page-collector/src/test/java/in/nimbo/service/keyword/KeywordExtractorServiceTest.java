package in.nimbo.service.keyword;

import in.nimbo.service.keyword.KeywordExtractorService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KeywordExtractorServiceTest {
    private static KeywordExtractorService keywordExtractorService;

    @BeforeClass
    public static void init() throws IOException {
        keywordExtractorService = new KeywordExtractorService();
    }

    @Test
    public void extractKeywordTest() {
        String text = "nimbo sahab. nimbo nimbo sahab nimbo.";
        Map<String, Integer> keywords = new HashMap<>();
        keywords.put("nimbo", 4);
        keywords.put("sahab", 2);
        Assert.assertEquals(keywords, keywordExtractorService.extractKeywords(text));
    }

    @Test
    public void stopWordsTest() {
        String text = "he him could that were was.";
        Map<String, Integer> keywords = new HashMap<>();
        Assert.assertEquals(keywords, keywordExtractorService.extractKeywords(text));
    }

    @Test
    public void moreThanFiveWordsTest() {
        String text = "nimbo sahab nimbo keenbo datapirate sahab nimbo jimbo keenbo nimbo datapirate keenbo nimroo jimbo";
        Map<String, Integer> keywords = new HashMap<>();
        keywords.put("nimbo", 4);
        keywords.put("keenbo", 3);
        keywords.put("sahab", 2);
        keywords.put("datapirate", 2);
        keywords.put("jimbo", 2);
        Assert.assertEquals(keywords, keywordExtractorService.extractKeywords(text));
    }

    @Test
    public void wordRootTest() {
        String text = "driving drove foot came";
        Map<String, Integer> keywords = new HashMap<>();
        keywords.put("drive", 2);
        keywords.put("come", 1);
        keywords.put("foot", 1);
        Assert.assertEquals(keywords, keywordExtractorService.extractKeywords(text));
    }
}
