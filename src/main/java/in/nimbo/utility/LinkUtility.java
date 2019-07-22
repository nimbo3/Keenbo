package in.nimbo.utility;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class LinkUtility {
    private LinkUtility() {
    }

    /**
     * extract domain from a url
     * url must be in absolute format
     * @param link link
     * @return domain of url without it's subdomains
     * @throws URISyntaxException if link is not a illegal url
     */
    public static String getMainDomain(String link) throws URISyntaxException {
        URI uri = new URI(link);
        String host = uri.getHost();
        String[] hostParts = host.split("\\.");
        return hostParts[hostParts.length - 2] + "." + hostParts[hostParts.length - 1];
    }

    /**
     * check whether a url is a valid url or not
     * @param link url
     * @return true if url is a valid url
     */
    public static boolean isValidUrl(String link) {
        try {
            URI uri = new URL(link).toURI();
            return uri.getHost() != null && uri.getHost().split("\\.").length >= 2;
        } catch (MalformedURLException | URISyntaxException | NullPointerException e) {
            return false;
        }
    }
}
