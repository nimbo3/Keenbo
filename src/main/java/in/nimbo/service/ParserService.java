package in.nimbo.service;

import in.nimbo.config.AppConfig;
import in.nimbo.entity.Page;
import in.nimbo.exception.ParseLinkException;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

public class ParserService {
    private AppConfig appConfig;

    public ParserService(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    public Page parse(String siteLink) {
        List<String> links = new ArrayList<>();
        try {
            Connection.Response response = Jsoup.connect(siteLink)
                    .ignoreContentType(true)
                    .userAgent("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0")
                    .referrer("http://www.google.com")
                    .timeout(appConfig.getJsoupTimeout())
                    .followRedirects(true)
                    .execute();
            Document document = response.parse();
            Elements elements = document.getElementsByTag("a");
            for (Element element : elements) {
                if (!element.absUrl("href").isEmpty())
                    links.add(element.absUrl("href"));
            }
            return new Page(document.html(), links);
        } catch (MalformedURLException e) {
            throw new ParseLinkException("Illegal url format: " + siteLink, e);
        } catch (IOException e) {
            throw new ParseLinkException("Unable to parse page with jsoup", e);
        }
    }
}
