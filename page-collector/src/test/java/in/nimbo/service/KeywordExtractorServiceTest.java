package in.nimbo.service;

import in.nimbo.service.keyword.KeywordExtractorService;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KeywordExtractorServiceTest {
    @Test
    public void extractKeywordTest() {
        String text = "nimbo sahab. nimbo nimbo sahab nimbo.";
        Map<String, Integer> keywords = new HashMap<>();
        keywords.put("nimbo", 4);
        keywords.put("sahab", 2);
        Assert.assertEquals(keywords, KeywordExtractorService.extractKeywords(text));
    }

    @Test
    public void stopWordsTest() {
        String text = "he him could that were was.";
        Map<String, Integer> keywords = new HashMap<>();
        Assert.assertEquals(keywords, KeywordExtractorService.extractKeywords(text));
    }

    @Test
    public void moreThanFiveWordsTest() {
        String text = "nimbo sahab nimbo keenbo datapirates sahab nimbo jimbo keenbo nimbo datapirates keenbo nimroo jimbo";
        Map<String, Integer> keywords = new HashMap<>();
        keywords.put("nimbo", 4);
        keywords.put("keenbo", 3);
        keywords.put("sahab", 2);
        keywords.put("datapirate", 2);
        keywords.put("jimbo", 2);
        Assert.assertEquals(keywords, KeywordExtractorService.extractKeywords(text));
    }

    @Test
    public void wordRootTest() {
        String text = "driving drove foots came";
        Map<String, Integer> keywords = new HashMap<>();
        keywords.put("drive", 2);
        keywords.put("come", 1);
        keywords.put("foot", 1);
        Assert.assertEquals(keywords, KeywordExtractorService.extractKeywords(text));
    }
}
