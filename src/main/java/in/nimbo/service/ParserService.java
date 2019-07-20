package in.nimbo.service;

import in.nimbo.config.AppConfig;
import in.nimbo.entity.Page;
import in.nimbo.exception.ParseLinkException;
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

    public Optional<Page> parse(String siteLink) {
        List<String> links = new ArrayList<>();
        try {
            Connection.Response response = Jsoup.connect(siteLink)
                    .ignoreContentType(true)
                    .userAgent("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0")
                    .timeout(appConfig.getJsoupTimeout())
                    .followRedirects(true)
                    .ignoreContentType(true)
                    .execute();
            if (!response.contentType().contains("text/html"))
                return Optional.empty();
            Document document = response.parse();
            Elements elements = document.getElementsByTag("a");
            for (Element element : elements) {
                String absUrl = element.absUrl("href");
                if (!absUrl.isEmpty() && !absUrl.matches("mailto:.*")) {
                    links.add(absUrl);
                }
            }
            return Optional.of(new Page(document.html(), links));
        } catch (MalformedURLException e) {
            throw new ParseLinkException("Illegal url format: " + siteLink, e);
        } catch (HttpStatusException e) {
            logger.error("Response is not OK. Url: {}, StatusCode: {}" + e.getUrl(), e.getStatusCode());
            return Optional.empty();
        } catch (IOException e) {
            throw new ParseLinkException("Unable to parse page with jsoup: " + siteLink, e);
        }
    }
}
