package in.nimbo.common.service;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import in.nimbo.common.config.ProjectConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.LanguageDetectException;
import in.nimbo.common.exception.ParseLinkException;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Meta;
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
    private Logger parserLogger = LoggerFactory.getLogger("parser");
    private Logger appLogger = LoggerFactory.getLogger("crawler");
    private ProjectConfig projectConfig;

    public ParserService(ProjectConfig projectConfig) {
        this.projectConfig = projectConfig;
    }

    /**
     * return document of page if it is present
     *
     * @param link link of site
     * @return
     */
    Optional<Document> getDocument(String link) {
        try {
            Connection.Response response = Jsoup.connect(link)
                    .userAgent(projectConfig.getJsoupUserAgent())
                    .timeout(projectConfig.getJsoupTimeout())
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
            parserLogger.warn("Server certificate verification failed: {}", link);
        } catch (UnknownHostException e) {
            parserLogger.warn("Could not resolve host: {}", link);
        } catch (MalformedURLException | IllegalArgumentException e) {
            parserLogger.warn("Illegal url format: {}", link);
        } catch (HttpStatusException e) {
            parserLogger.warn("Response is not OK. Url: \"{}\" StatusCode: {}", e.getUrl(), e.getStatusCode());
        } catch (SocketTimeoutException e) {
            parserLogger.warn("Connection time out with jsoup: {}", link);
        } catch (StringIndexOutOfBoundsException | IOException e) {
            parserLogger.warn("Unable to parse page with jsoup: {}", link);
        }
        return Optional.empty();
    }

    /**
     * @param document document contain a site contents
     * @return list of all anchors in a document
     */
    private Set<Anchor> getAnchors(Document document) {
        Set<Anchor> anchors = new HashSet<>();
        Elements linkElements = document.getElementsByTag("a");
        Map<String, Integer> map = new HashMap<>();
        for (Element linkElement : linkElements) {
            String absUrl = linkElement.absUrl("href");
            if (!absUrl.isEmpty() && !absUrl.matches("mailto:.*")
                    && LinkUtility.isValidUrl(absUrl) && !linkElement.text().equals("")) {
                try {
                    String normalizedUrl = LinkUtility.normalize(absUrl);
                    map.merge(normalizedUrl, 1, Integer::sum);
                    if (map.get(normalizedUrl) > 1) {
                        normalizedUrl += "#" + map.get(normalizedUrl);
                    }
                    anchors.add(new Anchor(normalizedUrl, linkElement.text().toLowerCase()));
                } catch (MalformedURLException e) {
                    parserLogger.warn("Unable to normalize link: {}", absUrl);
                }
            }
        }
        return anchors;
    }

    /**
     * @param document document contain a site contents
     * @return list of all metas in a document
     */
    List<Meta> getMetas(Document document) {
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
    String getTitle(Document document) {
        Elements titleElements = document.getElementsByTag("title");
        if (!titleElements.isEmpty()) {
            return titleElements.get(0).text();
        } else {
            return "";
        }
    }

    /**
     * @param text text
     * @return true if text is in English
     */
    boolean isEnglishLanguage(String text) {
        try {
            Detector detector = DetectorFactory.create();
            detector.append(text);
            detector.setAlpha(0);
            return detector.getProbabilities().stream()
                    .anyMatch(x -> x.lang.equals("en") && x.prob > projectConfig.getEnglishProbability());
        } catch (LangDetectException e) {
            throw new LanguageDetectException(e);
        }
    }

    /**
     * crawl a site and return it's content as a page
     *
     * @param link link of site
     * @return page if able to crawl page
     */
    public Page getPage(String link) {
        try {
            Optional<Document> documentOptional = getDocument(link);
            if (!documentOptional.isPresent()) {
                throw new ParseLinkException("JSoup parse exception");
            }
            Document document = documentOptional.get();
            String pageContentWithoutTag = document.text().replace("\n", " ");
            if (pageContentWithoutTag.isEmpty()) {
                parserLogger.warn("There is no content for site: {}", link);
            } else if (isEnglishLanguage(pageContentWithoutTag)) {
                Set<Anchor> anchors = getAnchors(document);
                List<Meta> metas = getMetas(document);
                String title = getTitle(document);
                if (title.isEmpty()) {
                    title = link;
                }
                return new Page(link, title, pageContentWithoutTag, anchors, metas, 1.0);
            }
        } catch (MalformedURLException e) {
            appLogger.warn("Unable to reverse link: {}", link);
        } catch (LanguageDetectException e) {
            parserLogger.warn("Cannot detect language of site: {}", link);
        }
        throw new ParseLinkException();
    }
}
