package in.nimbo.utility;

import java.net.URI;
import java.net.URISyntaxException;

public class LinkUtility {
    private LinkUtility() {
    }

    /**
     * @param link link
     * @return reversed link(only domain)
     */
    public static String reverseLink(String link){
        return link;
    }

    /**
     * extract domain from a url
     * url must be in absolute format
     * @param link link
     * @return domain of url without it's subdomains
     * @throws URISyntaxException if link is not a illegal url
     */
    public static String getMainDomain(String link) throws URISyntaxException {
        try {
            URI uri = new URI(link);
            String host = uri.getHost();
            String[] hostParts = host.split("\\.");
            return hostParts[hostParts.length - 2] + "." + hostParts[hostParts.length - 1];
        } catch (IndexOutOfBoundsException | NullPointerException e) {
            throw new URISyntaxException(link, "unable to detect host of url");
        }
    }

    /**
     * check whether a url is a valid url or not
     * @param link url
     * @return true if url is a valid url
     */
    public static boolean isValidUrl(String link) {
        try {
            URI uri = new URI(link);
            return uri.getHost() != null && uri.getHost().split("\\.").length >= 2;
        } catch (URISyntaxException | NullPointerException e) {
            return false;
        }
    }
}
