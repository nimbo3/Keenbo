package in.nimbo.service;

import in.nimbo.config.AppConfig;
import in.nimbo.entity.Anchor;
import in.nimbo.entity.Meta;
import in.nimbo.utility.LinkUtility;
import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ParserService {
    private Logger logger = LoggerFactory.getLogger(LinkUtility.class);
    private AppConfig appConfig;

    public ParserService(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    /**
     * return document of page if it is present
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
        } catch (MalformedURLException e) {
            logger.warn("Illegal url format: {}", link);
        } catch (HttpStatusException e) {
            logger.warn("Response is not OK. Url: \"{}\" StatusCode: {}", e.getUrl(), e.getStatusCode());
        } catch (IOException e) {
            logger.warn("Unable to parse page with jsoup: {}", link);
        }
        return Optional.empty();
    }

    /**
     *
     * @param document document contain a site contents
     * @return list of all anchors in a document
     */
    public List<Anchor> getAnchors(Document document) {
        List<Anchor> anchors = new ArrayList<>();
        Elements linkElements = document.getElementsByTag("a");
        for (Element linkElement : linkElements) {
            String absUrl = linkElement.absUrl("href");
            if (!absUrl.isEmpty() && !absUrl.matches("mailto:.*")
                    && LinkUtility.isValidUrl(absUrl)) {
                anchors.add(new Anchor(absUrl, linkElement.text()));
            }
        }
        return anchors;
    }

    /**
     *
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
     *
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
}
