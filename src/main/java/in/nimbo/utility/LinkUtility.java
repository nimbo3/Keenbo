package in.nimbo.utility;

import java.net.URI;
import java.net.URISyntaxException;

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
}
