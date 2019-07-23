package in.nimbo.service;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.cybozu.labs.langdetect.Language;
import in.nimbo.config.AppConfig;
import in.nimbo.entity.Page;
import in.nimbo.exception.LanguageDetectException;
import in.nimbo.utility.LinkUtility;
import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.Doc;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ParserService {
    private Logger logger = LoggerFactory.getLogger(LinkUtility.class);
    private AppConfig appConfig;

    public ParserService(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    public Optional<Page> parse(String siteLink) {
        List<String> links = new ArrayList<>();
        try {
            Optional<Document> documentOptional = getDocument(siteLink);
            if (!documentOptional.isPresent())
                return Optional.empty();
            Document document = documentOptional.get();
            String pageContent = document.text();
            if (isEnglishLanguage(pageContent)) {
                Elements elements = document.getElementsByTag("a");
                for (Element element : elements) {
                    String absUrl = element.absUrl("href");
                    if (!absUrl.isEmpty() && !absUrl.matches("mailto:.*")
                            && LinkUtility.isValidUrl(absUrl)) {
                        links.add(absUrl);
                    }
                }
                return Optional.of(new Page(pageContent, links));
            }
        } catch (LanguageDetectException e) {
            logger.warn("cannot detect language of site : {}", siteLink);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return Optional.empty();
    }

    public Optional<Document> getDocument(String siteLink) {
        try {
            Connection.Response response = Jsoup.connect(siteLink)
                    .userAgent(appConfig.getJsoupUserAgent())
                    .timeout(appConfig.getJsoupTimeout())
                    .followRedirects(true)
                    .ignoreContentType(true)
                    .execute();
            if (response.contentType() != null &&
                    !response.contentType().contains("text/html")) {
                return Optional.empty();
            } else {
                return Optional.of(response.parse());
            }
        } catch (MalformedURLException e) {
            logger.warn("Illegal url format: {}", siteLink);
        } catch (HttpStatusException e) {
            logger.warn("Response is not OK. Url: \"{}\" StatusCode: {}", e.getUrl(), e.getStatusCode());
        } catch (IOException e) {
            logger.warn("Unable to parse page with jsoup: {}", siteLink);
        }
        return Optional.empty();
    }

    /**
     * @param text text
     * @return true if text is in English
     */
    private boolean isEnglishLanguage(String text) {
        try {
            DetectorFactory.loadProfile("profiles");
            Detector detector = DetectorFactory.create();
            detector.append(text);
            return detector.detect().equals("en");
        } catch (LangDetectException e) {
            throw new LanguageDetectException(e);
        }
    }
}
