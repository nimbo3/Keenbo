package in.nimbo.service;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import in.nimbo.config.AppConfig;
import in.nimbo.entity.Anchor;
import in.nimbo.entity.Meta;
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

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.*;

public class ParserService {
    private Logger logger = LoggerFactory.getLogger(LinkUtility.class);
    private AppConfig appConfig;

    public ParserService(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    /**
     * return document of page if it is present
     *
     * @param link link of site
     * @return
     */
    public Optional<Document> getDocument(String link) {
        try {
            Connection.Response response = Jsoup.connect(link)
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
        } catch (SSLHandshakeException e) {
            logger.warn("Server certificate verification failed: {}", link);
        } catch (UnknownHostException e) {
            logger.warn("Could not resolve host: {}", link);
        } catch (MalformedURLException | IllegalArgumentException e) {
            logger.warn("Illegal url format: {}", link);
        } catch (HttpStatusException e) {
            logger.warn("Response is not OK. Url: \"{}\" StatusCode: {}", e.getUrl(), e.getStatusCode());
        } catch (SocketTimeoutException e) {
            logger.warn("Connection time out with jsoup: {}", link);
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn("Unable to parse page with jsoup: {}", link);
        }
        return Optional.empty();
    }

    /**
     * @param document document contain a site contents
     * @return list of all anchors in a document
     */
    public Set<Anchor> getAnchors(Document document) {
        Set<Anchor> anchors = new HashSet<>();
        Elements linkElements = document.getElementsByTag("a");
        for (Element linkElement : linkElements) {
            String absUrl = linkElement.absUrl("href");
            if (!absUrl.isEmpty() && !absUrl.matches("mailto:.*")
                    && LinkUtility.isValidUrl(absUrl)) {
                anchors.add(new Anchor(LinkUtility.normalize(absUrl), linkElement.text()));
            }
        }
        return anchors;
    }

    /**
     * @param document document contain a site contents
     * @return list of all metas in a document
     */
    public List<Meta> getMetas(Document document) {
        List<Meta> metas = new ArrayList<>();
        Elements metaElements = document.getElementsByTag("meta");
        for (Element metaElement : metaElements) {
            String name = metaElement.attr("name");
            String content = metaElement.attr("content");
            if (name != null && content != null && !name.isEmpty() && !content.isEmpty()) {
                Meta meta = new Meta(name, content);
                metas.add(meta);
            }
        }
        return metas;
    }

    /**
     * @param document document contain a site contents
     * @return title of document and empty if there is no title
     */
    public String getTitle(Document document) {
        Elements titleElements = document.getElementsByTag("title");
        if (titleElements.size() > 0) {
            return titleElements.get(0).text();
        } else {
            return "";
        }
    }

    /**
     * @param text text
     * @return true if text is in English
     */
    public boolean isEnglishLanguage(String text) {
        try {
            Detector detector = DetectorFactory.create();
            detector.append(text);
            detector.setAlpha(0);
            return detector.getProbabilities().stream()
                    .anyMatch(x -> x.lang.equals("en") && x.prob > appConfig.getEnglishProbability());
        } catch (LangDetectException e) {
            throw new LanguageDetectException(e);
        }
    }
}
