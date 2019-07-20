package in.nimbo.utility;

import java.net.URI;
import java.net.URISyntaxException;

public class LinkUtility {
    private LinkUtility() {
    }

    public static String getDomain(String link) throws URISyntaxException {
        URI uri = new URI(link);
        String host = uri.getHost();
        return host.startsWith("www.") ? host.substring(4) : host;
    }
}
