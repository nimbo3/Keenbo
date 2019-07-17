package in.nimbo.entity;

import java.net.MalformedURLException;
import java.net.URL;

public class Link {
    private String protocol;
    private String host;
    private int port;
    private String uri;
    private String domain;

    public String getDomain() {
        return domain;
    }

    public Link(String link) throws MalformedURLException {
        URL url = new URL(link);
        protocol = url.getProtocol();
        host = url.getHost();
        port = url.getPort() != -1 ? url.getPort() : protocol.equals("http") ? 80 : 443;
        uri = url.getPath();
        String[] hostPart = host.split("\\.");
        this.domain = hostPart[hostPart.length - 2] + "." + hostPart[hostPart.length - 1];
    }

    public String getProtocol() {
        return protocol;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUri() {
        return uri;
    }
}
